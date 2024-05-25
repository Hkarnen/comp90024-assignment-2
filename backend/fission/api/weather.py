import utils

def get_stations(es):
    # distinct stations
    query = {
        "size": 0, # don't return any documents
        "aggs": {
            "stations": {
                "composite": {
                    "size": 100, # max number of stations
                    "sources": [
                        {"wmo": {"terms": {"field": "wmo"}}},
                        {"name": {"terms": {"field": "name.keyword"}}},
                        {"lat": {"terms": {"field": "lat"}}},
                        {"lon": {"terms": {"field": "lon"}}},
                        {"precise_lat": {"terms": {"field": "precise_lat"}}},
                        {"precise_lon": {"terms": {"field": "precise_lon"}}},
                    ]
                }
            }
        },
        "_source": False
    }
    
    res = es.search(index="new_weather_data", body=query)
    results = res["aggregations"]["stations"]["buckets"]
    stations = [result["key"] for result in results]

    # rename precise_lat and precise_lon to lat and lon
    for station in stations:
        if "precise_lat" in station:
            station["lat"] = station.pop("precise_lat")
        if "precise_lon" in station:
            station["lon"] = station.pop("precise_lon")
        
    
    wmos = [station["wmo"] for station in stations]
    # check whether all WMO IDs are unique
    if len(wmos) == len(set(wmos)):
        return {"stations": stations}
    
    # if there are stations with the same WMO ID, keep only one of them
    print("Warning: some stations have the same WMO ID")
    unique_stations = []
    for wmo in set(wmos):
        matching = [d["name"] for d in stations if d["wmo"] == wmo]
        unique_stations.append({"wmo": wmo, "name": matching[0]})
         
    return {"station_wmos": unique_stations}


def aggregate_observations(es, station_id, year=None, month=None, day=None, hour=None):
    query = {
        "query": {
            "bool": { "filter": [ { "term": { "wmo": { "value": station_id } } }, ] }
        },
        "size": 0,
        "aggs": {
            "avg_temperature": {
                "avg": {
                    "field": "air_temp"
                }
            },
            "max_temperature": {
                "max": {
                    "field": "air_temp"
                }
            },
            "min_temperature": {
                "min": {
                    "field": "air_temp"
                }
            },
            "avg_wind_speed_kmh": {
                "avg": {
                    "field": "wind_spd_kmh"
                }
            },
            "max_wind_speed_kmh": {
                "max": {
                    "field": "wind_spd_kmh"
                }
            },
            "min_wind_speed_kmh": {
                "min": {
                    "field": "wind_spd_kmh"
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
        start_date, end_date = utils.get_date_limits(year, month, day, hour)
        query["query"]["bool"]["filter"].append({
            "range": {
                "local_date_time_full": {
                    "gte": start_date.strftime("%Y%m%d%H%M%S"),
                    "lte": end_date.strftime("%Y%m%d%H%M%S"),
                    "format": "yyyyMMddHHmmss"
                }
            }
        })
     
    print(query)
    
    res = es.search(index="new_weather_data", body=query).body["aggregations"]
    res = { key: res[key]["value"] for key in res }
    
    if year:
        res["date_filter"] = {
            "start": start_date.strftime("%Y-%m-%d %H:%M:%S"),
            "end": end_date.strftime("%Y-%m-%d %H:%M:%S")
        }
        
    return res
