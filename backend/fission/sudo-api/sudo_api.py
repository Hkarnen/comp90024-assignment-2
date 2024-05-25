# import weather
import logging
import json
from flask import request
import elasticsearch

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def config(k):
    with open(f'/configs/default/shared-data/{k}', 'r') as f:
        value = f.read().strip()
        logging.debug("Read value for config key '%s': %s", k, value)
        return value
    
def connect_elasticsearch():
    logging.debug("Connecting to Elasticsearch...")
    try:
        es = elasticsearch.Elasticsearch(
            f"https://elasticsearch-master.elastic.svc.cluster.local:9200",
            http_auth=(config('ES_USERNAME'), config('ES_PASSWORD')),
            verify_certs=False
        )
        logging.debug("Connected to Elasticsearch successfully.")
        return es
    except Exception as e:
        logging.error("Failed to connect to Elasticsearch: %s", e)
        raise

def get_simplified_response(res):
    # Process the results to build the desired dictionary
    result_dict = {}
    for hit in res['hits']['hits']:
        source = hit['_source']
        sa2_code = source[' sa2_code_2021']
        result_dict[sa2_code] = {
            "Total_Dwellings": source[' total_dwellings'],
            "num_mot_veh_per_dwg_tot_dwgs": source['num_mot_veh_per_dwg_tot_dwgs']
        }

    return result_dict

def get_vehicles():
    logging.info("Retrieving freeways from Elasticsearch...")
    try:
        es = connect_elasticsearch()

        # Define the query
        query = {
            "query": {
                "match_all": {}
            },
            "size": 1000  # Adjust the size if needed
        }
        # Execute the query
        res = es.search(index="sudo-vehicle-register", body=query)

        vehicle_data = get_simplified_response(res)

        logging.info("Successfully retrieved freeways.")
        return json.dumps(vehicle_data)
    except Exception as e:
        logging.error("Failed to retrieve freeways: %s", e)
        return json.dumps({"error": str(e)})

