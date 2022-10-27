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


def parse_args(args):
    print('printing')
    print(args)
    print('printed')

    parser = argparse.ArgumentParser(
        description="parallelcollgs - Parallel OpenSeach / OData API interactions with multiple Copernicus collaborative ground segments"
    )
    parser.add_argument("--user", help="Datahub username", type=str)
    parser.add_argument("--password", help="Datahub password", type=str)
    parser.add_argument("--url", help="Datahub URL", type=str)
    parser.add_argument("--timeout", help="DHuS mirror timeout", type=float)

    return parser.parse_args(args)


def main():
    args = vars(parse_args(sys.argv[1:]))
    print(args.keys())


if __name__ == "__main__":
    main()
