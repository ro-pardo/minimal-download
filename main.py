import yaml
import csv
import json
from os import path, mkdir
from requests.exceptions import RequestException
from time import sleep
import getpass
import re
import datetime
from concurrent.futures import (
    ThreadPoolExecutor,
    ProcessPoolExecutor,
    wait,
    as_completed,
)
from sentinelsat import SentinelAPI, InvalidChecksumError, SentinelAPIError, read_geojson, geojson_to_wkt


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


# Match 100 km resolution UTM grid ID
def _match_UTM(x):
            return bool(re.match("^[0-9]{1,2}[A-Z]{3}$", x))


def datetime_parser(dct):
    """JSON datetime parser
    """
    for k, v in list(dct.items()):
        if isinstance(v, str) and re.search("[0-9]{4}-[0-9]{1,2}-[0-9]{2} ", v):
            try:
                dct[k] = datetime.datetime.strptime(v, "%Y-%m-%d %H:%M:%S.%f")
            except:
                try:
                    dct[k] = datetime.datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
                except:
                    pass
    return dct


class SentinelAPIManager(object):

    def printme(self):
        print("_connections")
        print(self._connections)

    # Based on the extension received processes it as WTK UTM or CSV
    def _load_meta(self, fpath):
        """Automatically choose a parsing method and return parsed data
        """
        fext = path.splitext(fpath)[-1]
        if fext == ".csv":
            tile_ids = self.load_csv(fpath)
            return self.manager.search(tile_ids)
        elif fext == ".json":
            return self.load_json(fpath)
        elif fext == ".geojson":
            footprint = geojson_to_wkt(read_geojson(fpath))
            return self.manager.search(footprint)
        elif fext in {".yaml", ".yml"}:
            return self.load_yaml(fpath)
        elif not fext and self.is_UTM(fpath):
            return self.manager.search(fpath.split(","))
        else:
            raise ValueError(
                "%s is not a valid target. Expected UTM grid, CSV, "
                "YAML or JSON file" % fpath
            )

    # Return True if all elemets of a comma seperated string are UTM grid ids
    def is_UTM(string):
        return all(list(map(_match_UTM, string.split(","))))

    # Load YAML file to dict
    def load_yaml(fpath):
        with open(fpath, "r") as f:
            data = yaml.safe_load(f)
        return data

    # Load first column of a CSV file
    def load_csv(fpath, skip_header=True):
        with open(fpath, "r") as f:
            reader = csv.reader(f)
            ids = []
            if skip_header:
                next(reader, None)
            for row in reader:
                ids.append(row[0])
        return ids

    # Load JSON file, cast dates to datetime values
    def load_json(fpath):
        with open(fpath, "rb") as f:
            data = json.load(f, object_hook=datetime_parser)
        return data

    # Handle connection to SentinelAPI
    def _connect_thread(self, mirror, **kwargs):
        url = self.config["mirrors"][mirror]["url"]
        user = self.config["mirrors"][mirror]["user"]
        password = self.config["mirrors"][mirror]["password"]
        timeout = self.config["timeout"]

        try:
            api = SentinelAPI(
                user, password, api_url=url, show_progressbars=False, timeout=timeout
            )
            count = api.count(**kwargs)
            return (api, count)
        except (SentinelAPIError, RequestException) as err:
            self.logger.info(
                "Request to mirror '%s' raised '%s'", mirror, err.__class__.__name__
            )
            for trial in range(self.config["retry"]):
                try:
                    self.logger.info(
                        "Trying again [%d/%d] ...", trial + 1, self.config["retry"]
                    )
                    api = SentinelAPI(
                        user,
                        password,
                        api_url=url,
                        show_progressbars=False,
                        timeout=timeout,
                    )
                    count = api.count(**kwargs)
                except (SentinelAPIError, RequestException) as err:
                    self.logger.info(
                        "Request to mirror '%s' raised '%s'",
                        mirror,
                        err.__class__.__name__,
                    )
                    sleep(1)
                    continue
                else:
                    return (api, count)
            return None

    # Connect to all mirrors
    def _connect(self):
        args = {
            "date": (self.config["date"]["from"], self.config["date"]["to"]),
            "cloudcoverpercentage": (0, self.config["cloud"]),
            "platformname": self.config["platformname"],
            "producttype": self.config["producttype"],
        }
        with ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(self._connect_thread, mirror, **args): mirror
                for mirror in self.config["mirrors"]
            }
            for future in as_completed(futures):
                mirror = futures[future]
                res = future.result()
                if res:
                    self.api[mirror] = res[0]
                    self.config["mirrors"][mirror]["num_available"] = res[1]

    def _parse_args(self, **kwargs):
        """Parse input to class constructor
        """

        test = kwargs.get("test")
        if test:
            self.config["test"] = test
        elif "test" not in self.config:
            self.config["test"] = [
                "31UES",  # Bruxelles
                "32UQD",  # Berlin
                "32VNM",  # Oslo
                "33UWP",  # Vienna
                "34SGH",  # Athens
                "35VLG",  # Helsinki
                "32TQM",
            ]  # Rome

        base = kwargs.get("base")
        if base:
            self.config["base"] = base
        elif "base" not in self.config:
            self.config["base"] = "./"
        if not path.exists(self.config["base"]):
            mkdir(self.config["base"])

        from_date = kwargs.get("from")
        to_date = kwargs.get("to")
        if "date" not in self.config:
            self.config["date"] = {}
        if from_date:
            self.config["date"]["from"] = from_date
        elif "from" not in self.config["date"]:
            self.config["date"]["from"] = "NOW-356DAY"
        if to_date:
            self.config["date"]["to"] = to_date
        elif "to" not in self.config["date"]:
            self.config["date"]["to"] = "NOW"

        cloud = kwargs.get("cloud")
        if cloud:
            self.config["cloud"] = cloud
        elif "cloud" not in self.config:
            self.config["cloud"] = 10.0

        timeout = kwargs.get("timeout")
        if timeout:
            self.config["timeout"] = timeout
        elif "timeout" not in self.config:
            self.config["timeout"] = 15.0

        retry = kwargs.get("retry")
        if retry:
            self.config["retry"] = retry
        elif "retry" not in self.config:
            self.config["retry"] = 5

        parallel = kwargs.get("parallel")
        if parallel:
            self.config["parallel"] = parallel
        elif "parallel" not in self.config:
            self.config["parallel"] = 4
        connections = kwargs.get("connections")
        if connections:
            self.config["connections"] = connections
        elif "connections" not in self.config:
            self.config["connections"] = 2

        platformname = kwargs.get("platformname")
        if platformname:
            self.config["platformname"] = "Sentinel-%d" % platformname
        elif "platformname" not in self.config:
            self.config["platformname"] = "Sentinel-2"

        producttype = kwargs.get("producttype")
        if producttype:
            self.config["producttype"] = producttype
        elif "producttype" not in self.config:
            if self.config["platformname"] == "Sentinel-1":
                self.config["producttype"] = "SLC"
            elif self.config["platformname"] == "Sentinel-2":
                self.config["producttype"] = "S2MSI1C"
            elif self.config["platformname"] == "Sentinel-3":
                self.config["producttype"] = "SR_1_SRA___"

        user = kwargs.get("user")
        password = kwargs.get("password")
        url = kwargs.get("url")
        if user:
            self.config["mirrors"] = {}
            self.config["mirrors"]["Custom"] = {}
            self.config["mirrors"]["Custom"]["user"] = user
            if password:
                self.config["mirrors"]["Custom"]["password"] = password
            else:
                password = getpass()
                if not password:
                    raise ValueError("Password can not be empty")
                self.config["mirrors"]["Custom"]["password"] = password
            if url:
                self.config["mirrors"]["Custom"]["url"] = url
            else:
                self.config["mirrors"]["Custom"][
                    "url"
                ] = "https://scihub.copernicus.eu/dhus"

    def __init__(self, config_file=None, **kwargs):
        # Config file
        if config_file:
            self.config = self.load_yaml(config_file)
        else:
            self.config = {}
        self.config_file = config_file

        # get config params
        # values passed via kwargs override values read from config file
        self._parse_args(**kwargs)

        self._connections = {name: 0 for name in self.config["mirrors"]}

        # TODO Used to be a ProductDownloadList class
        self._download_list = []

        self._proc_executor = ProcessPoolExecutor()
        self._proc_futures = {}

        self.api = {}
        self._connect()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

    manager = SentinelAPIManager(
        user='rpardomeza', password='12c124ccb2', cloud=None, platformname=2, producttype='S2MSI1C'
    )
    print(manager)