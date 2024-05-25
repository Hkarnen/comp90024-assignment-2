import weather
import air_quality
import json
from flask import request
import elasticsearch

def config(k):
    with open(f'/configs/default/shared-data/{k}', 'r') as f:
        return f.read().strip()
    
def connect_elasticsearch():
    return elasticsearch.Elasticsearch(
        f"https://elasticsearch-master.elastic.svc.cluster.local:9200",
        http_auth=(config('ES_USERNAME'), config('ES_PASSWORD')),
        verify_certs=False
    )

def weather_get_stations():
    es = connect_elasticsearch()
    return json.dumps(weather.get_stations(es))

def weather_aggregate_observations():
    es = connect_elasticsearch()

    try:
        station_id = request.headers["X-Fission-Params-station-id"]
        print(f"station_id: {station_id}")
    except KeyError:
        return "Error: station_id not provided", 400
    
    year = request.args.get("year", None)
    month = request.args.get("month", None)
    day = request.args.get("day", None)
    hour = request.args.get("hour", None)
    
    return json.dumps(weather.aggregate_observations(es, station_id, year, month, day, hour)) 
    
def air_quality_get_stations():
    es = connect_elasticsearch()
    return json.dumps(air_quality.get_stations(es))

def air_quality_aggregate_observations():
    es = connect_elasticsearch()

    try:
        station_id = request.headers["X-Fission-Params-station-id"]
        print(f"station_id: {station_id}")
    except KeyError:
        return "Error: station_id not provided", 400
    
    year = request.args.get("year", None)
    month = request.args.get("month", None)
    day = request.args.get("day", None)
    hour = request.args.get("hour", None)
    
    return json.dumps(air_quality.aggregate_observations(es, station_id, year, month, day, hour))

