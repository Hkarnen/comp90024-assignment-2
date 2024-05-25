import utils
import pytz

def get_stations(es):
    query = {
        "size": 0, # don't return any documents
        "aggs": {
            "stations": {
                "composite": {
                    "size": 100, # max number of stations
                    "sources": [
                        {"site_id": {"terms": {"field": "site_id.keyword"}}},
                        {"site_name": {"terms": {"field": "site_name.keyword"}}},
                        {"latitude": {"terms": {"field": "latitude"}}},
                        {"longitude": {"terms": {"field": "longitude"}}},
                    ]
                }
            }
        },
        "_source": False
    }

    res =  es.search(index="air_quality_data", body=query)
    results = res["aggregations"]["stations"]["buckets"]
    stations = [result["key"] for result in results]
    return {"stations": stations}


def aggregate_observations(es, station_id, year=None, month=None, day=None, hour=None):
    query = {
        "query": {
            "bool": { "filter": [ { "term": { "site_id.keyword": { "value": station_id } } }, ] }
        },
        "size": 0,
        "aggs": {
            "avg_pm25": {
                "avg": {
                    "field": "averageValue"
                }
            },
            "max_pm25": {
                "max": {
                    "field": "averageValue"
                }
            },
            "min_pm25": {
                "min": {
                    "field": "averageValue"
                }
            },
        }
    }
    
    if year:
        try:
            year = int(year)
        except ValueError:
            return {"error": "year must be an integer"}
        
    if month:
        try:
            month = int(month)
        except ValueError:
            return {"error": "year must be an integer"}
        
    if day:
        try:
            day = int(day)
        except ValueError:
            return {"error": "year must be an integer"}
    
    if hour:
        try:
            hour = int(hour)
        except ValueError:
            return {"error": "year must be an integer"}
    
    if year:
        # times are utc, other apis are in local time, so we need to convert
        # to Melbourne time for the easy of this assignment
        start_date, end_date = utils.get_date_limits(year, month, day, hour)

        melb_tz = pytz.timezone('Australia/Melbourne')
        start_date = melb_tz.localize(start_date).astimezone(pytz.utc)
        end_date = melb_tz.localize(end_date).astimezone(pytz.utc)

        query["query"]["bool"]["filter"].append({
            "range": {
                "since": {
                    "gte": start_date.isoformat(),
                    "lte": end_date.isoformat()
                }
            }
        })
    
    res = es.search(index="air_quality_data", body=query).body["aggregations"]
    res = { key: res[key]["value"] for key in res }
    
    if year:
        res["date_filter"] = {
            "start": start_date.astimezone(melb_tz).strftime("%Y-%m-%d %H:%M:%S"),
            "end": end_date.astimezone(melb_tz).strftime("%Y-%m-%d %H:%M:%S")
        }
        
    return res
