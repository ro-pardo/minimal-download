import datetime
from collections import Counter

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