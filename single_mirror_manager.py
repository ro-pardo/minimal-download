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
import sys
import argparse
from sentinelsat import SentinelAPI, InvalidChecksumError, SentinelAPIError, read_geojson, geojson_to_wkt


class SentinelAPIManager(object):

    def _parse_args(self, **kwargs):

        user = kwargs.get("user")
        password = kwargs.get("password")
        url = kwargs.get("url")

        if user and password and url:
            print('all variables for connection available')
        else:
            raise ValueError('No connection provided')

        self.config["mirror"] = {}
        self.config["mirror"]["user"] = user
        self.config["mirror"]["password"] = password
        self.config["mirror"]["url"] = url

        cloud = kwargs.get("cloud")
        if cloud:
            self.config["cloud"] = cloud
        elif "cloud" not in self.config:
            self.config["cloud"] = 10.0

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

        timeout = kwargs.get("timeout")
        if timeout:
            self.config["timeout"] = timeout
        elif "timeout" not in self.config:
            self.config["timeout"] = 15.0

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

    def __init__(self, **kwargs):

        self.config = {}

        # get config params
        # values passed via kwargs override values read from config file
        self._parse_args(**kwargs)

        self._connections = {'mirror': 0}

        # TODO Used to be a ProductDownloadList class
        self._download_list = []

        self._proc_executor = ProcessPoolExecutor()
        self._proc_futures = {}

        self.api = {}
        # self._connect()
        self._connect_hard(kwargs.get("user"), kwargs.get("password"), kwargs.get("url"))

        print('revision point')

    def _connect_hard(self, user, password, url):

        with ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(self.hard_connection, user, password, url)
            }
            for future in as_completed(futures):
                res = future.result()
                if res:
                    self.api['mirror'] = res[0]
                    self.config["mirror"]["num_available"] = res[1]

    def hard_connection(self, user, password, url):
        global args
        try:
            args = {
                "date": (self.config["date"]["from"], self.config["date"]["to"]),
                "cloudcoverpercentage": (0, self.config["cloud"]),
                "platformname": self.config["platformname"],
                "producttype": self.config["producttype"],
            }
            api = SentinelAPI(
                user, password, api_url=url, show_progressbars=False, timeout=self.config["timeout"]
            )
            count = api.count(**args)
            return (api, count)

        except (SentinelAPIError, RequestException) as err:
            print(err)
        #     self.logger.info(
        #         "Request to mirror '%s' raised '%s'", name, err.__class__.__name__
        #     )
        #     for trial in range(self.config["retry"]):
        #         try:
        #             self.logger.info(
        #                 "Trying again [%d/%d] ...", trial + 1, self.config["retry"]
        #             )
        #             api = SentinelAPI(
        #                 user,
        #                 password,
        #                 api_url=url,
        #                 show_progressbars=False,
        #                 timeout=self.config["timeout"],
        #             )
        #             count = api.count(**args)
        #         except (SentinelAPIError, RequestException) as err:
        #             self.logger.info(
        #                 "Request to mirror '%s' raised '%s'",
        #                 name,
        #                 err.__class__.__name__,
        #             )
        #             sleep(1)
        #             continue
        #         else:
        #             return (api, count)
        #     return None


def parse_args(args):
    print('printing')
    print(args)
    print('printed')

    parser = argparse.ArgumentParser(
        description="Single mirror parallel download"
    )
    parser.add_argument("--user", help="Datahub username", type=str)
    parser.add_argument("--password", help="Datahub password", type=str)
    parser.add_argument("--url", help="Datahub URL", type=str)
    parser.add_argument("--timeout", help="DHuS mirror timeout", type=float)

    return parser.parse_args(args)


def main():
    # Argument as dictionary
    args = vars(parse_args(sys.argv[1:]))
    print(args.keys())

    # API connection to Sentinel as object
    manager = SentinelAPIManager(
        user=args.get('user'), password=args.get('password'), hard_mirror='Invoked', url=args.get('url'),
        cloud=None, platformname=2, producttype='S2MSI1C'
    )


if __name__ == "__main__":
    main()
