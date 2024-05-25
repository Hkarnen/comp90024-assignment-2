import requests
import time
import elasticsearch
from flask import current_app
import urllib3
from urllib3.exceptions import InsecureRequestWarning
import sys

EPA_URL = "https://gateway.api.epa.vic.gov.au/environmentMonitoring/v1/"

def config(k):
    with open(f'/configs/default/shared-data/{k}', 'r') as f:
        return f.read().strip()

def epa_get(path: str, params: dict, retries=3, delay=1) -> requests.Request:
    """
    Handles GET requests to the EPA API. Retries on 429 status code 
    """
    for attempt in range(retries):
        req = requests.get(
            url=EPA_URL + path,
            params=params,
            headers={
                "X-API-Key": "afe31938fe2d413a9ba7cbba7a3cebe8", #config('EPA_API_KEY'),
                # bug in API which doesn't like the auto python header
                "User-Agent": "curl/8.4.0",
            },
        )
        if req.status_code == 429:
            # couldn't access even after retries attempts
            if attempt == (retries - 1):
                raise Exception
            time.sleep(delay)
        else:
            req.raise_for_status()
            return req


def handle_response(resp) -> dict:
    return resp.json()


def get_sites() -> dict:
    """
    Gets all available sites from the EPA API which provide air quality data
    """
    resp = epa_get("sites", params={"environmentalSegment": "air"})
    return handle_response(resp)


def get_site(site_id: str) -> dict:
    """
    Gets the air measurements for a specific site
    """
    path = f"sites/{site_id}/parameters"
    resp = epa_get(path, params={})
    return handle_response(resp)


def get_pm2p5(site_id: str) -> float:
    """
    Extracts the last hourly PM2.5 reading from a site 
    """
    location_data = get_site(site_id)

    if location_data["siteType"] != "Standard":
        return None

    site_name = location_data.get("siteName", None)

    coords = location_data.get("geometry", {}).get("coordinates", None)
    if coords is not None:
        lat = coords[0]
        long = coords[1]
    else:
        lat = None
        long = None

    result = None

    for parameter in location_data["parameters"]:
        if parameter["name"] == "PM2.5":
            for time_series in parameter["timeSeriesReadings"]:
                if time_series["timeSeriesName"] == "1HR_AV":
                    for reading in time_series["readings"]:
                        since = reading["since"]
                        until = reading["until"]
                        
                        # the seems to be the the indication the observation is incomplete
                        if reading["totalSample"] == 0:
                            return result
                        obs_id = f"{site_id}--{since}--{until}"
                        
                        result = {
                            "obs_id": obs_id,
                            "site_id": site_id,
                            "site_name": site_name,
                            "latitude": lat,
                            "longitude": long,
                            "averageValue": reading["averageValue"],
                            "unit": reading["unit"],
                            "since": since,
                            "until": until,
                            "confidence": reading["confidence"],
                            "totalSample": reading["totalSample"],
                            "healthAdvice": reading["healthAdvice"],
                        }
                        break
            break

    return result


def main():
    current_app.logger.info("Starting EPA harvester")
    urllib3.disable_warnings(category=InsecureRequestWarning)

    try:
        sites = get_sites()
    except requests.exceptions.HTTPError as e:
        current_app.logger.error(f"Failed to get list of sites: HTTP {e.response.status_code} -- {e.response.reason}")
        return 'fail'
    except Exception as e:
        current_app.logger.error(f"Failed to get list of sites: {e}")
        return 'fail'
    
    current_app.logger.info(f"Retrieved {len(sites['records'])} sites")

    site_ids = list(map(lambda x: x["siteID"], sites["records"]))
    current_app.logger.info(f"Retrieved {len(site_ids)} site IDs")

    pm25_results = []
    for site in site_ids:
        time.sleep(1/3)  # 6 requests per second. Function handles retries
        try:
            pm25 = get_pm2p5(site)
            current_app.logger.info(f"Retrieved PM2.5 data for site {site}")
        except requests.exceptions.HTTPError as e:
            current_app.logger.error(f"Failed to get PM2.5 data for site {site}: HTTP {e.response.status_code} -- {e.response.reason}")
            continue

        if pm25 is not None:
            pm25_results.append(pm25)

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
    
    air_quality_index = "air_quality_data"
    
    for result in pm25_results:
        exists = es.exists(index=air_quality_index, id=result["obs_id"]).body
        if not exists:
            es.index(index=air_quality_index, id=result["obs_id"], body=result)
            current_app.logger.info(f'Indexed observation {result["obs_id"]}')

    current_app.logger.info("Finished EPA harvester")

    return 'ok'
