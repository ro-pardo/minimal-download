import csv
import json
import os
import yaml
import datetime
import re
from collections import Counter
import zipfile

def get_season_year(idate):
    """From a given JSON response get Year and Season

    Parameters
    ----------
    idate : datetime.date
        datetime value for product ingestion date

    Returns
    -------
    tuple
        tuple with (season, year)
    """
    year = idate.year
    date = idate.date()
    seasons = {
        "last_winter": (datetime.date(year, 1, 1), datetime.date(year, 3, 20)),
        "spring": (datetime.date(year, 3, 21), datetime.date(year, 6, 20)),
        "summer": (datetime.date(year, 6, 21), datetime.date(year, 9, 22)),
        "autumn": (datetime.date(year, 9, 23), datetime.date(year, 12, 20)),
        "winter": (datetime.date(year, 12, 21), datetime.date(year, 12, 31)),
    }
    if seasons["spring"][0] <= date <= seasons["spring"][1]:
        return ("spring", year)
    if seasons["summer"][0] <= date <= seasons["summer"][1]:
        return ("summer", year)
    if seasons["autumn"][0] <= date <= seasons["autumn"][1]:
        return ("autumn", year)
    if seasons["winter"][0] <= date <= seasons["winter"][1]:
        return ("winter", year)
    if seasons["last_winter"][0] <= date <= seasons["last_winter"][1]:
        return ("winter", year - 1)
    raise ValueError("Unable to find season for date {}".format(idate))

def get_year_season_map(meta):
    """Map product UUIDs to year and season

    Parameters
    ----------
    meta : dict
        JSON data hub response

    Returns
    -------
    dict
        Nested dict with year and season as keys
    """
    year_season_map = {}
    for uuid in meta:
        if "ingestiondate" not in meta[uuid]:
            continue
        season, year = get_season_year(meta[uuid]["ingestiondate"])
        if year not in year_season_map:
            year_season_map[year] = {
                "spring": [],
                "summer": [],
                "autumn": [],
                "winter": [],
            }
        year_season_map[year][season].append(uuid)
    return year_season_map

def get_year_season_selection(meta):
    """
    Select product with:
        - lowest cloudcoverpercentage and
        - highest filesize
    per year and season

    Parameters
    ----------
    meta : dict
        JSON data hub response

    Returns
    -------
    dict
        Nested dict with year and season as keys
        Contains only one UUID per year/season
    """
    selection = {}
    year_season_map = get_year_season_map(meta)
    for year in year_season_map:
        selection[year] = {}
        for season in year_season_map[year]:
            if not year_season_map[year][season]:
                continue
            cloud = {}
            size = {}
            for uuid in year_season_map[year][season]:
                cloud[uuid] = meta[uuid]["cloudcoverpercentage"]
                size[uuid] = float(meta[uuid]["size"].split()[0])
            rank_cloud = Counter(
                {
                    val[0]: 1.25 * idx
                    for idx, val in enumerate(
                        sorted(cloud.items(), key=lambda m: m[1], reverse=True), start=1
                    )
                }
            )
            rank_size = Counter(
                {
                    val[0]: idx
                    for idx, val in enumerate(
                        sorted(size.items(), key=lambda m: m[1]), start=1
                    )
                }
            )
            rank = rank_cloud + rank_size
            best = rank.most_common(1)[0][0]
            selection[year].update({season: best})
    return selection

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