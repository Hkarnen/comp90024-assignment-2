import requests
from elasticsearch import Elasticsearch
from flask import request, current_app, jsonify
import urllib3
from urllib3.exceptions import InsecureRequestWarning

def config(k):
    with open(f'/configs/default/shared-data/{k}', 'r') as f:
        return f.read().strip()

def main():
    urllib3.disable_warnings(category=InsecureRequestWarning)
    current_app.logger.info("Fetching traffic data")
    
    # Define the URL and headers for the API request
    url = "https://data-exchange-api.vicroads.vic.gov.au/opendata/variable/freewaytraveltime/v1/traffic"
    headers = {
        'Cache-Control': 'no-cache',
        'Ocp-Apim-Subscription-Key': config('TRAFFIC_API_KEY')
    }

    # Make the request to the API
    response = requests.get(url, headers=headers)
    # Ignore if not successful
    if response.status_code != 200:
        current_app.logger.error(f"Failed to fetch data: HTTP {response.status_code} -- {response.reason}")
        return jsonify({'error': 'Failed to fetch data', 'status_code': response.status_code}), response.status_code

    data = response.json()

    es = Elasticsearch(
        'https://elasticsearch-master.elastic.svc.cluster.local:9200',
        verify_certs=False,
        http_auth=(config('ES_USERNAME'), config('ES_PASSWORD'))
    )

    # Process only the important features for analysis
    processed_data = []
    for feature in data['features']:
        
        properties = feature['properties']
        obs_id = f"{properties.get('id')}---{properties.get('publishedTime')}"
        exists = es.exists(index="traffic-data", id=obs_id).body
        if exists:
            continue
        # Removed id check, since id here is just freeway id.
        relevant_data = {
            'obs_id': obs_id,
            'freewayName': properties.get('freewayName'),
            'segmentName': properties.get('segmentName'),
            'publishedTime': properties.get('publishedTime'),
            'condition': properties.get('condition'),
            'actualTravelTime': properties.get('actualTravelTime'),
            'averageSpeed': properties.get('averageSpeed'),
            'congestionIndex': properties.get('congestionIndex'),
            'geometry': feature.get('geometry')
        }
        processed_data.append(relevant_data)
        es.index(index="traffic-data", id=obs_id, body=relevant_data)
        current_app.logger.info(f"Indexed data for freeway: {properties.get('freewayName')} at time {properties.get('publishedTime')}")
    
    if processed_data:
        current_app.logger.info(f"Processed data: {processed_data[0]}")
        return jsonify(processed_data), 200
    else:
        return jsonify({'message': 'No data processed'}), 200
