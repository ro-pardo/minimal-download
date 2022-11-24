import csv
import json
import yaml
from os import path
import os
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
from product_download import ProductDownload
import zipfile
from download_state import DownloadState

def _match_year(x):
    """Match Year string
    """
    return bool(re.match("^[0-9]{1,4}$", str(x)))


def is_selection(meta):
    """
    Return True if parameter "meta" is a nested dict with:
        - MGRS grid as first key
        - Year as second key
        - ["spring", "summer", "autumn", "winter"] as third key
    """
    for utm in meta:
        if not _match_UTM(utm):
            return False
        for year in meta[utm]:
            if not _match_year(year):
                return False
            return any(
                [
                    season in meta[utm][year]
                    for season in ["spring", "summer", "autumn", "winter"]
                ]
            )


def get_keys(meta):
    """
    Extract (utm, UUID) pairs from:
        - ordinary data hub JSON response dict
        - product selection dict
    """
    keys = []
    selection = is_selection(meta)
    for utm in meta:
        for val in meta[utm]:
            if selection:
                for season in meta[utm][val]:
                    keys.append((utm, meta[utm][val][season]))
            else:
                keys.append((utm, val))
    return keys


def order_by_utm(response):
    """Order data hub JSON response by MGRS grid id
    """
    utms = {}
    for uuid in response:
        if "tileid" in response[uuid]:
            utm = response[uuid]["tileid"]
            if utm not in utms:
                utms[utm] = {}
            utms[utm].update({uuid: response[uuid]})
        else:
            utm = re.search(
                "_T([0-9]{1,2}[A-Z]{3})_", response[uuid]["filename"]
            ).groups(1)[0]
            response[uuid]["tileid"] = utm
            if utm not in utms:
                utms[utm] = {}
            utms[utm].update({uuid: response[uuid]})
    return utms

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


def unzip(fpath, dest="."):
    """Unzip file

    Parameters
    ----------
    fpath : str
        Zip file path
    dest : str
        Folder to extract to

    Returns
    -------
    str
        Directory of unziped files
    """
    print('path', fpath)
    print(os.path.getsize(fpath))

    with open(fpath, "rb") as f:
        zf = zipfile.ZipFile(f)
        out = zf.namelist()[0]
        zf.extractall(path=dest)
    return os.path.join(dest, out)


class Query(object):

    def unzip_callback(self, future):
        """Unzip a downloaded product

        Attach this via add_done_callback to a Future object

        Parameters
        ----------
        future : concurrent.futures.Future object
            Object that the callback was attached to
            Future status is either Done or Canceled
        """
        download = self.downloads.find(future)
        self.connections -= 1
        try:
            response = future.result()
        except Exception as err:
            self.state = DownloadState.FAILED
            self.logger.info(
                "[%d/%d] UUID %s | Download failed",
                download.index[0],
                download.index[1],
                download.uuid,
            )
            self.logger.error(str(err))
            return
        self.logger.info(
            "[%d/%d] UUID %s | Download complete (%s) @ %.2f MB/s",
            download.index[0],
            download.index[1],
            download.uuid,
            download.mirror,
            download.speed,
        )
        print('response')
        print(response)
        img_dir = os.path.split(response["path"])[0]
        _future = self._proc_executor.submit(unzip, response["path"], img_dir)
        download.state = DownloadState.EXTRACT_ACTIVE
        fname = response["title"] + ".SAFE"
        # download.safe_path = os.path.join(img_dir, fname)
        _future.add_done_callback(download._unzip_callback)
        self._proc_futures[future] = fname

    def _download_thread(self, mirror, uuid, utm):
        """Download a Copernicus product

        Parameters
        ----------
        mirror : str
            Mirror name, key in self.config["mirrors"]
        uuid : str
            Product UUID
        utm : str
            MGRS tile id
            'None' if platformname not Sentinel-2
        """
        retry = self.retry
        if utm:
            img_dir = os.path.join(self.img_dir, utm)
        else:
            img_dir = self.img_dir
        if not os.path.exists(img_dir):
            os.mkdir(img_dir)
        api = self.api
        try:
            return api.download(uuid, img_dir)
        except (RequestException, SentinelAPIError, InvalidChecksumError) as err:
            self.logger.info("UUID %s | Raised '%s'", uuid, err.__class__.__name__)
            for trial in range(retry):
                try:
                    self.logger.info(
                        "UUID %s | Trying again '%s' [%d/%d] ...",
                        uuid,
                        mirror,
                        trial + 1,
                        retry,
                    )
                    return api.download(uuid, img_dir)
                except (
                        RequestException,
                        SentinelAPIError,
                        InvalidChecksumError,
                ) as err:
                    self.logger.info(
                        "UUID %s | Raised '%s'", uuid, err.__class__.__name__
                    )
                    sleep(10)
                    continue
            self.logger.error("UUID %s | Unable to download from '%s'", uuid, mirror)
            return None

    def get(self, meta):
        """Download and unzip raw data

        Maximize download speed by:
            - Trying to open as many connections as possible (see "parallel"
              and "connections" config options)
            - Always pick the fastest available mirror (given mirrors have
              been ranked before)

        Files are automatically unziped once a download completes. Unzip jobs
        are run in parallel via concurrent.futures.ProcessPoolExecutor.

        For Sentinel-2, data is organized as follows:

            $BASE/img/$UTM/$PRODUCT

        where $BASE is the base directory (see config.yaml / -d flag) and
        where $UTM is the respective MGRS tile ID and
        where $PRODUCT is the downloaded product zip/SAFE file

        For Sentinel-1 and 3, data is organized as follows:

            $BASE/img/$PRODUCT

        Parameters
        ----------
        meta : dict
            Sentinel-2: Mapping of MGRS tile to query response dict
            Sentinel-1/3: Mapping of UUID to query response dict
        """
        tic = perf_counter()
        self.logger.info("Starting product download")

        if True:  # self.config["platformname"] == "Sentinel-2":
            keys = get_keys(meta)
            uuids = [uuid for utm, uuid in keys]
            utm_map = {uuid: utm for utm, uuid in keys}
            retry_map = {uuid: 0 for _, uuid in keys}
        else:
            uuids = list(meta.keys())
            retry_map = {uuid: 0 for uuid in uuids}
        num_products = len(uuids)
        info = OrderedDict()

        # schedule all products
        download_list = self.downloads
        download_list.clear()
        for idx, uuid in enumerate(uuids, start=1):
            if True:  # self.config["platformname"] == "Sentinel-2":
                utm = utm_map[uuid]
                download_list.append(ProductDownload(uuid, (idx, num_products), utm))
            else:
                download_list.append(ProductDownload(uuid, (idx, num_products)))

        print('Imprimiendo ', len(download_list))
        for elem in download_list:
            print(elem)

        with ThreadPoolExecutor(max_workers=self.parallel) as executor:
            while not download_list.all_downloaded():
                for download in download_list.get_scheduled():
                    if not download.state == DownloadState.SCHEDULED:
                        continue

                    # TODO Used to be the one with more connections available
                    # download.mirror = self.find_mirror(download.uuid)
                    download.mirror = self.api
                    if download.mirror == "NONE":
                        if retry_map[download.uuid] >= self.retry:
                            download.state = DownloadState.FAILED
                            self.logger.info(
                                "[%d/%d] UUID %s | Download failed, retry limit exceeded",
                                download.index[0],
                                num_products,
                                download.uuid,
                            )
                        else:
                            retry_map[download.uuid] += 1
                        continue
                    if download.mirror == "BUSY":
                        # wait indefinately
                        continue
                    future = executor.submit(
                        self._download_thread,
                        download.mirror,
                        download.uuid,
                        download.utm,
                    )
                    self.connections += 1
                    download.register(future)
                    future.add_done_callback(self.unzip_callback)
                    self.logger.info(
                        "[%d/%d] UUID %s | Download starting (%s)",
                        download.index[0],
                        num_products,
                        download.uuid,
                        download.mirror,
                    )
                    if len(self.downloads.get_active()) >= self.parallel:
                        self.downloads.wait_for_completed()
                sleep(2)
            self.logger.info("")
            for future in as_completed(self._proc_futures):
                fname = self._proc_futures[future]
                try:
                    self.logger.info("PRODUCT %s [x]", fname)
                except FileExistsError as err:
                    self.logger.info("PRODUCT %s [o]", fname)
            self._proc_futures.clear()

        elapsed = perf_counter() - tic
        self.logger.info("\nProduct download completed in %f sec", elapsed)
        self.logger.info("Total size: %s MB", download_list.size())
        num_failed = len(download_list.get_failed())
        if num_failed > 0:
            self.logger.info("Failed: %d / %d", num_failed, num_products)
        self.downloads.clear()
        self.logger.info("Shutting down processor pool. This might take some time..")
        self._proc_executor.shutdown()


    def _query_thread(self, **kwargs):
        api = self.api
        retry = 2

        query_kwargs = {'platformname': 'Sentinel-2', 'producttype': 'S2MSI1C', 'date': ('NOW-14DAYS', 'NOW'),
                        'tileid': kwargs.get('tileid')}
        try:
            sentinel_response = api.query(**kwargs)
            # sentinel_response = api.query(**query_kwargs)
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
            i=0
            for future in as_completed(futures):
                # name = futures[future]
                name = 'fake_mirror' + str(i)
                i = i+1
                res = future.result()
                if res:
                    for uid in res:
                        res[uid]["mirror"] = name
                    response[name] = res
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
                                    utms = order_by_utm(response)
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
        elapsed = perf_counter() - tic
        self.logger.info("\nProduct search completed in %f sec", elapsed)
        return res

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
        self.downloads = kwargs.get("downloads")
        self._proc_executor = kwargs.get("executor")
        self._proc_futures = kwargs.get("futures")

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
        self.retry = 0
        self.connections = 0

        self.base_dir = os.getcwd()  # self.config["base"]
        self.img_dir = os.path.join(self.base_dir, "images")
        if not os.path.exists(self.img_dir):
            os.makedirs(self.img_dir)
            self.logger.info("Created %s", self.img_dir)
        self.logger.info("Downloading to %s\n", self.img_dir)

        metadata = self._load_meta(self.order)
        print('metadata')
        print(metadata)
        print('\n')

        # selection = self.select(metadata)
        # print('selection')
        # print(selection)
        self.get(metadata)

        print('lets download')
