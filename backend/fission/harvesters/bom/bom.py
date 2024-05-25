from functools import reduce
import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
from io import StringIO
import re
from flask import current_app
import elasticsearch
import urllib3
from urllib3.exceptions import InsecureRequestWarning


STATIONS_INDEX = "https://reg.bom.gov.au/climate/data/lists_by_element/stations.txt"
VIC_WEATHER_STATIONS = "https://reg.bom.gov.au/vic/observations/vicall.shtml"

def config(k):
    with open(f'/configs/default/shared-data/{k}', 'r') as f:
        return f.read().strip()


def parse_stations_table(raw_stations_table) -> pd.DataFrame:
    """
    Cleans the FWF table of BOM stations and returns a pandas DataFrame
    """

    stations_txt = raw_stations_table.decode("utf-8")
    # removing preamble and copyright stuff at the end
    stations_txt = stations_txt.splitlines()[2:-6]
    stations_txt = "\n".join(stations_txt)
    stations = pd.read_fwf(StringIO(stations_txt))
    # remove the '----' row entry
    stations = stations.drop([0])
    stations["WMO"] = stations["WMO"].replace("..", pd.NA)
    stations = stations.loc[stations["WMO"].notna(), ["WMO", "Site name", "Lat", "Lon"]]
    stations.set_index("WMO", inplace=True)
    return stations


def get_stations_locations(stations_index_url):
    """
    Get a dictionary of BOM stations with WMO as the key. Primarily interested in latitude and longitude
    """

    req = requests.get(stations_index_url)
    
    try:
        req.raise_for_status()
    except requests.exceptions.HTTPError as e:
        current_app.logger.error(
            f"Failed to connect to {stations_index_url}: HTTP {e.response.status_code} -- {e.response.reason}"
        )
        return {}

    stations = parse_stations_table(req.content)

    current_app.logger.info(f"Retrieved {len(stations)} stations")

    return stations.to_dict(orient="index")


def get_vic_weather_stations_urls(vic_stations_url):
    """
    Extracts the list of BOM weather stations in Victoria, ignoring portable weather stations
    """
    req = requests.get(vic_stations_url)
    req.raise_for_status()
    
    soup = bs(req.text, 'html.parser')

    # need to exclude the urls for portable weather stations as this is unlikely to be
    # useful for our purposes
    excluded_links = soup.find("table", id="tPORT").find_all("a", href=True)
    excluded_hrefs = {link["href"] for link in excluded_links}

    # now find all links that contain the string "/products/IDV"
    links = soup.find_all("a", href=re.compile("/products/IDV"))

    # filter out the excluded links
    links = [link for link in links if link.get("href") not in excluded_hrefs]

    # extract the id from the href
    id_re = re.compile(r"products/(.+)\.shtml")
    id_strings = [id_re.search(link.get("href")).group(1) for link in links]
    return [f"https://reg.bom.gov.au/fwo/{id}.json" for id in id_strings]


def get_weather_data(url):
    """
    Get array of weather observations from a weather station
    """
    req = requests.get(url)
    try:
        req.raise_for_status()
    except requests.exceptions.HTTPError as e:
        current_app.logger.error(f"Failed to get weather observations from {url}: HTTP {e.response.status_code} -- {e.response.reason}")
        return None

    return req.json()["observations"]["data"]



def main():
    current_app.logger.info("Fetching weather data")
    urllib3.disable_warnings(category=InsecureRequestWarning)
    
    try:
        station_urls = get_vic_weather_stations_urls(VIC_WEATHER_STATIONS)
        current_app.logger.info(f"Retrieved {len(station_urls)} Vic weather station URLs")
    except requests.exceptions.HTTPError as e:
        current_app.logger.critical(f"Failed to get station URLs: HTTP {e.response.status_code} -- {e.response.reason}")
        return 'fail'
    
    stations_weather_obs = [get_weather_data(url) for url in station_urls]
    # if a request fails, get_weather_data returns None
    stations_weather_obs = [obs for obs in stations_weather_obs if obs is not None]
    
    if not stations_weather_obs:
        current_app.logger.error("No weather observations retrieved")
        return 'fail'

    # flatten the list of lists
    all_obs = reduce(lambda x, y: x + y, stations_weather_obs)
    
    # get more precise lat and lon for each station
    station_locations = get_stations_locations(STATIONS_INDEX)

    # append a more precise lat and lon to each observation
    for obs in all_obs:
        station_number = str(obs.get("wmo"))
        if station_number in station_locations:
            lat = station_locations[station_number]["Lat"]
            lon = station_locations[station_number]["Lon"]
            obs["precise_lat"] = lat
            obs["precise_lon"] = lon
            
    try:
        es = elasticsearch.Elasticsearch(
            # url should also be in config
            'https://elasticsearch-master.elastic.svc.cluster.local:9200',
            verify_certs=False,
            http_auth=(config('ES_USERNAME'), config('ES_PASSWORD'))
        )
    except Exception as e:
        current_app.logger.fatal(f"Failed to connect to Elasticsearch: {e}")
        return 'fail'
    
    # had to reindex because the index was created with the wrong mapping
    es_weather_index = "new_weather_data"

    for obs in all_obs:
        obs_id = f'{obs["wmo"]}--{obs["aifstime_utc"]}'
        already_exists = es.exists(index=es_weather_index, id=obs_id).body
        if not already_exists:
            es.index(index=es_weather_index, id=obs_id, body=obs)
            current_app.logger.info(f"Indexed observation {obs_id}")
            
    current_app.logger.info("Finished indexing weather observations")
            
    return 'ok'
        
