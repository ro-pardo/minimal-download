import csv
import json
import yaml
from os import path
import datetime
import re
from sentinelsat import read_geojson, geojson_to_wkt
from collections import OrderedDict, Counter
from time import perf_counter, sleep
from concurrent.futures import (
    ThreadPoolExecutor,
    ProcessPoolExecutor,
    wait,
    as_completed,
)
import logging
from sys import stdout
from sentinelsat import SentinelAPI, InvalidChecksumError, SentinelAPIError, read_geojson, geojson_to_wkt
from requests.exceptions import RequestException


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


def is_utm(string):
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


class Query(object):

    def _query_thread(self, **kwargs):
        api = self.api
        retry = 2

        query_kwargs = {'platformname': 'Sentinel-2', 'producttype': 'S2MSI1C', 'date': ('NOW-14DAYS', 'NOW'),
                        'tileid': kwargs.get('tileid')}
        try:
            sentinel_response = api.query(**kwargs)
            return sentinel_response
        except (RequestException, SentinelAPIError) as err:
            self.logger.info(
                "Request to mirror '%s' raised '%s'", 'Greece API', err.__class__.__name__
            )
            for trial in range(retry):
                try:
                    self.logger.info(
                        "Trying again '%s' [%d/%d] ...", 'Greece API', trial + 1, retry
                    )
                    return api.query(**kwargs)
                except (RequestException, SentinelAPIError) as err:
                    self.logger.info(
                        "Request to mirror '%s' raised '%s'",
                        'Greece API',
                        err.__class__.__name__,
                    )
                    sleep(1)
                    continue
            self.logger.error("Unable to query mirror '%s'", 'Greece API')
            return None

    # Wrap SentinelAPI query method
    def query(self, merge=True, ignore_conf=False, **kwargs):
        for k in kwargs:
            print(k, ': ', kwargs.get(k))
        response = OrderedDict()
        conf_args = kwargs
        self.logger.debug("Querying DHuS")

        with ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(self._query_thread, **conf_args)
            }
            for future in as_completed(futures):
                print(future.result())
                # name = futures[future]
                # res = future.result()
                # if res:
                #     for uid in res:
                #         res[uid]["mirror"] = name
                #     response[name] = res
        return response

    def search(self, targets):
        parallel = self.parallel
        self.logger.info("Starting product search\n")
        tic = perf_counter()
        res = OrderedDict()
        futures = {}
        with ThreadPoolExecutor(max_workers=parallel) as executor:
            if isinstance(targets, list):
                num_targets = len(targets)
                for idx, target in enumerate(targets, start=1):
                    print(idx, ': ', target)
                    if is_utm(target):
                        print(target, ' is UTM')
                        futures[executor.submit(self.query, tileid=target)] = (
                            idx,
                            target,
                        )
                    else:
                        print(target, ' is something else')
                        futures[executor.submit(self.query, area=target)] = (
                            idx,
                            target,
                        )
                    if idx % parallel == 0 or idx == num_targets:
                        for future in as_completed(futures):
                            _idx, _target = futures[future]
                            response = future.result()
                            if is_utm(_target):
                                res.update({_target: response})
                                self.logger.info(
                                    "[%d/%d] MGRS %s | %d products",
                                    _idx,
                                    num_targets,
                                    _target,
                                    len(response),
                                )
                            else:
                                self.logger.info("Footprint: %s\n", target)
                                if self.config["platformname"] == "Sentinel-2":
                                    # utms = utils.order_by_utm(response)
                                    utms = []
                                    for utm in utms:
                                        res.update({utm: utms[utm]})
                                        self.logger.info(
                                            "[%d/%d] MGRS %s | %d products",
                                            _idx,
                                            num_targets,
                                            utm,
                                            len(utms[utm]),
                                        )
                                else:
                                    for idx, uuid in enumerate(response, start=1):
                                        self.logger.info(
                                            "[%d/%d] UUID %s", idx, len(response), uuid
                                        )
                                        res.update({uuid: response[uuid]})
                                self.logger.info("")
                        futures.clear()

    def _load_meta(self, fpath):
        """Automatically choose a parsing method and return parsed data
        """
        fext = path.splitext(fpath)[-1]
        if fext == ".csv":
            tile_ids = load_csv(fpath)
            return self.search(tile_ids)
        elif fext == ".json":
            return load_json(fpath)
        elif fext == ".geojson":
            footprint = geojson_to_wkt(read_geojson(fpath))
            return self.search(footprint)
        elif fext in {".yaml", ".yml"}:
            return load_yaml(fpath)
        elif not fext and is_utm(fpath):
            return self.search(fpath.split(","))
        else:
            raise ValueError(
                "%s is not a valid target. Expected UTM grid, CSV, "
                "YAML or JSON file" % fpath
            )

    def _parse_args(self, **kwargs):
        self.api = kwargs.get("api")
        self.order = kwargs.get("order")

    def __init__(self, **kwargs):
        self.logger = logging.getLogger("single-mirror")
        if not self.logger.handlers:
            handler = logging.StreamHandler(stdout)
            formater = logging.Formatter("%(message)s")
            handler.setFormatter(formater)
            self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)

        self._parse_args(**kwargs)
        self.parallel = 2

        self._load_meta(self.order)

        print('lets download')
