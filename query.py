from os import path
import os
from collections import OrderedDict
from time import perf_counter, sleep
from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed,
)
import logging
from sys import stdout
from sentinelsat import InvalidChecksumError, SentinelAPIError, read_geojson, geojson_to_wkt
from requests.exceptions import RequestException
from product_download import ProductDownload
from download_state import DownloadState
from utils import get_year_season_selection, unzip, get_keys, is_utm, order_by_utm, load_csv, load_json, load_yaml


class Query(object):

    def down_a_level(self, meta):
        short_meta = OrderedDict()
        for idx, tile in enumerate(meta, start=1):
            for id2, mirror_url in enumerate(meta[tile], start=1):
                if(mirror_url):
                    short_meta[tile] = meta[tile][mirror_url]
                    # print('putting: ', tile, " : ", meta[tile][mirror_url])
                # short_meta.update({tile, meta[tile][mirror_url]})

        return short_meta


    def select(self, meta):

        selection = {}
        tic = perf_counter()
        self.logger.info("Starting product selection")
        tic = perf_counter()
        for idx, tile in enumerate(meta, start=1):
            self.logger.info("\nMGRS %s:", tile)
            if not meta[tile]:
                self.logger.info("Nothing to select")
                continue
            selection[tile] = get_year_season_selection(meta[tile])
            for year in selection[tile]:
                self.logger.info("")
                self.logger.info("    YEAR: %d", year)
                self.logger.info("    ----------")
                for season in selection[tile][year]:
                    if not selection[tile][year][season]:
                        self.logger.info("    %s: NONE", season.upper())
                        continue
                    uuid = selection[tile][year][season]
                    cloud = meta[tile][uuid]["cloudcoverpercentage"]
                    self.logger.info("    %s: %s (%f%%)", season.upper(), uuid, cloud)
        elapsed = perf_counter() - tic
        self.logger.info("\nProduct selection completed in %f sec", elapsed)
        return selection

    def unzip_callback(self, future):
        """Unzip a downloaded product

        Attach this via add_done_callback to a Future object

        Parameters
        ----------
        future : concurrent.futures.Future object
            Object that the callback was attached to
            Future status is either Done or Canceled
        """
        download = self._download_list.find(future)
        # TODO What will we do with the connections var
        # self.connections -= 1
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
        # print('response')
        # print(response)
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

        if self.manager.config["platformname"] == "Sentinel-2":
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
        download_list = self._download_list
        download_list.clear()
        for idx, uuid in enumerate(uuids, start=1):
            if self.manager.config["platformname"] == "Sentinel-2":
                utm = utm_map[uuid]
                download_list.append(ProductDownload(uuid, (idx, num_products), utm))
            else:
                download_list.append(ProductDownload(uuid, (idx, num_products)))

        # for elem in download_list:
        #     print("START ELEM")
        #     print(elem)
        #     print("END ELEM")
        # sys.exit()

        with ThreadPoolExecutor(max_workers=self.parallel) as executor:
            while not download_list.all_downloaded():
                for download in download_list.get_scheduled():
                    if not download.state == DownloadState.SCHEDULED:
                        continue

                    # TODO Used to be the one with more connections available
                    download.mirror = 'NOT USE THIS'
                    # download.mirror = self.find_mirror(download.uuid)
                    # download.mirror = self.api
                    # if download.mirror == "NONE":
                    #     if retry_map[download.uuid] >= self.retry:
                    #         download.state = DownloadState.FAILED
                    #         self.logger.info(
                    #             "[%d/%d] UUID %s | Download failed, retry limit exceeded",
                    #             download.index[0],
                    #             num_products,
                    #             download.uuid,
                    #         )
                    #     else:
                    #         retry_map[download.uuid] += 1
                    #     continue
                    # if download.mirror == "BUSY":
                    #     # wait indefinately
                    #     continue
                    future = executor.submit(
                        self._download_thread,
                        download.mirror,
                        download.uuid,
                        download.utm,
                    )
                    # self.connections += 1
                    download.register(future)
                    future.add_done_callback(self.unzip_callback)
                    self.logger.info(
                        "[%d/%d] UUID %s | Download starting (%s)",
                        download.index[0],
                        num_products,
                        download.uuid,
                        download.mirror,
                    )
                    if len(self._download_list.get_active()) >= self.parallel:
                        self._download_list.wait_for_completed()
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
        self._download_list.clear()
        self.logger.info("Shutting down processor pool. This might take some time..")
        self._proc_executor.shutdown()

    def _query_thread(self, **kwargs):
        api = self.api
        retry = 2

        try:
            sentinel_response = api.query(**kwargs)
            return sentinel_response
        except (RequestException, SentinelAPIError) as err:
            self.logger.info(
                "Request to mirror '%s' raised '%s'", 'Greece API', err.__class__.__name__
            )
            # for trial in range(retry):
            #     try:
            #         self.logger.info(
            #             "Trying again '%s' [%d/%d] ...", 'Greece API', trial + 1, retry
            #         )
            #         return api.query(**kwargs)
            #     except (RequestException, SentinelAPIError) as err:
            #         self.logger.info(
            #             "Request to mirror '%s' raised '%s'",
            #             'Greece API',
            #             err.__class__.__name__,
            #         )
            #         sleep(1)
            #         continue
            # self.logger.error("Unable to query mirror '%s'", 'Greece API')
            return None

    # Wrap SentinelAPI query method
    def query(self, ignore_conf=False, **kwargs):

        response = OrderedDict()

        if ignore_conf:
            conf_args = kwargs
        else:
            conf_args = {
                "date": (self.manager.config["date"]["from"], self.manager.config["date"]["to"]),
                "platformname": self.manager.config["platformname"],
                "producttype": self.manager.config["producttype"],
                **kwargs,
            }
            if self.manager.config["platformname"] == "Sentinel-2":
                conf_args["cloudcoverpercentage"] = (0, self.manager.config["cloud"])

        self.logger.debug("Querying DHuS")

        with ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(self._query_thread, **conf_args)
            }
            for future in as_completed(futures):
                name = self.manager.config['mirror']['url']
                res = future.result()
                if res:
                    for uid in res:
                        res[uid]["mirror"] = name
                    response[name] = res
        return response

    def search(self, targets):
        self.logger.info("Starting product search\n")
        tic = perf_counter()
        res = OrderedDict()
        futures = {}
        with ThreadPoolExecutor(max_workers=self.parallel) as executor:
            if isinstance(targets, list):
                num_targets = len(targets)
                for idx, target in enumerate(targets, start=1):
                    if is_utm(target):
                        # print(target, ' is UTM')
                        futures[executor.submit(self.query, tileid=target)] = (
                            idx,
                            target,
                        )
                    else:
                        print(target, ' is an Area')
                        futures[executor.submit(self.query, area=target)] = (
                            idx,
                            target,
                        )
                    if idx % self.parallel == 0 or idx == num_targets:
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
                                if self.manager.config["platformname"] == "Sentinel-2":
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

            # TODO add cases where only a String is received
        elapsed = perf_counter() - tic
        self.logger.info("\nProduct search completed in %f sec", elapsed)
        return res

    def _load_meta(self, fpath):
        """Automatically choose a parsing method and return parsed data
        """
        fext = path.splitext(fpath)[-1]
        if fext == ".csv":
            self.logger.info("Detected product as csv")
            tile_ids = load_csv(fpath)
            return self.search(tile_ids)
        elif fext == ".json":
            self.logger.info("Detected product as json")
            return load_json(fpath)
        elif fext == ".geojson":
            self.logger.info("Detected product as geojson")
            footprint = geojson_to_wkt(read_geojson(fpath))
            return self.search(footprint)
        elif fext in {".yaml", ".yml"}:
            self.logger.info("Detected product as yaml")
            return load_yaml(fpath)
        elif not fext and is_utm(fpath):
            self.logger.info("Detected product as UTM")
            return self.search(fpath.split(","))
        else:
            raise ValueError(
                "%s is not a valid target. Expected UTM grid, CSV, "
                "YAML or JSON file" % fpath
            )

    def _parse_args(self, **kwargs):
        self.manager = kwargs.get("manager")
        self.api = self.manager.api
        self.order = kwargs.get("order")

        self._download_list = self.manager.download_list
        self._proc_executor = self.manager.proc_executor
        self._proc_futures = self.manager.proc_futures

        self.retry = 0
        self.parallel = 4

    def _logger_init(self):
        self.logger = logging.getLogger("single-mirror")
        if not self.logger.handlers:
            handler = logging.StreamHandler(stdout)
            formater = logging.Formatter("%(message)s")
            handler.setFormatter(formater)
            self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)

    def _resolve_path(self):
        self.base_dir = os.getcwd()  # self.config["base"]
        self.img_dir = os.path.join(self.base_dir, "images")
        if not os.path.exists(self.img_dir):
            os.makedirs(self.img_dir)
            self.logger.info("Created %s", self.img_dir)
        self.logger.info("Application download path: %s\n", self.img_dir)

    def __init__(self, **kwargs):
        self._logger_init()
        self._parse_args(**kwargs)
        self._resolve_path()

    def execute(self):

        self.logger.debug("Getting the metadata for the order: " + str(self.order))
        metadata = self._load_meta(self.order)

        self.logger.debug("Metadata obtained, resumed as follows:")
        self.logger.debug(metadata)
        self.logger.debug('\n')

        short = self.down_a_level(metadata)

        self.logger.debug('Selecting best products available')
        selection = self.select(short)
        self.logger.debug(selection)

        self.logger.debug('\n')
        self.get(selection)