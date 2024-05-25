# import weather
import logging
import freeway
import json
from flask import request
import elasticsearch

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# def config(k):
#     with open(f'/configs/default/shared-data/{k}', 'r') as f:
#         return f.read().strip()
    
# def connect_elasticsearch():
#     return elasticsearch.Elasticsearch(
#         f"https://elasticsearch-master.elastic.svc.cluster.local:9200",
#         http_auth=(config('ES_USERNAME'), config('ES_PASSWORD')),
#         verify_certs=False
#     )

# def get_freeways():
#     es = connect_elasticsearch()
#     return json.dumps(freeway.get_freeways(es))

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

def get_freeways():
    logging.info("Retrieving freeways from Elasticsearch...")
    try:
        es = connect_elasticsearch()
        freeways_data = freeway.get_freeways(es)
        logging.info("Successfully retrieved freeways.")
        return json.dumps(freeways_data)
    except Exception as e:
        logging.error("Failed to retrieve freeways: %s", e)
        return json.dumps({"error": str(e)})

def aggregate_observations():
    logger.info("Retrieving freeways from Elasticsearch...")
    es = connect_elasticsearch()

    print(request.headers)

    print(request.args)

    logging.info("Request headers: %s", request.headers)

    logging.info("Request arguments: %s", request.args)
    
    try:
        freewayName = request.headers["X-Fission-Params-FreewayName"]
        logging.info("Freeway name: %s", freewayName)
    except KeyError:
        logging.error("Error: freewayName not provided")
        return "Error: freewayName not provided", 400
    
    year = request.args.get("year", None)
    month = request.args.get("month", None)
    day = request.args.get("day", None)
    hour = request.args.get("hour", None)
    

    logging.info("Year: %s", year)

    result = freeway.aggregate_observations(es, freewayName, year, month, day, hour)
    logger.info("Aggregation result: %s", result)
    
    return json.dumps(result)
