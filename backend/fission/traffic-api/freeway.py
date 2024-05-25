import logging
import utils

# Set up logging
logging.basicConfig(level=logging.DEBUG)

def create_simplified_response(res):
    '''
    Function that helps create a simplified response for the api to output
    '''
    try:
        max_congestion = res['max_congestion']['value']
    except (KeyError, TypeError):
        max_congestion = None

    try:
        segment = res['top_segments']['buckets'][0]
        segment_name = segment['top_hit']['hits']['hits'][0]['_source']['segmentName']
        actual_travel_time = segment['top_hit']['hits']['hits'][0]['_source']['actualTravelTime']
        geometry = segment['top_hit']['hits']['hits'][0]['_source'].get('geometry', None)
        if geometry is not None:
            geometry_type = geometry.get('type', 'None')
            coordinates = geometry.get('coordinates', [])
        else:
            geometry_type = 'None'
            coordinates = []
    except (IndexError, KeyError, TypeError):
        segment_name = None
        actual_travel_time = None
        geometry_type = 'None'
        coordinates = []

    simplified_response = {
        "max_congestion_index": max_congestion,
        "segment_name": segment_name,
        "actual_travel_time": actual_travel_time,
        "geometry_type": geometry_type,
        "coordinates": coordinates
    }

    return simplified_response

def get_freeways(es):
    
    # distinct stations
    query = {
        "size": 0,  # We do not want any documents, just aggregations
        "aggs": {
            "unique_freewayNames": {
                "terms": {
                    "field": "freewayName.keyword",
                    "size": 10  
                }
            }
        }
    }

    logging.info("Executing Elasticsearch query...")
    res = es.search(index="traffic-data", body=query)
    logging.debug("Elasticsearch response: %s", res)
    
    results = res["aggregations"]["unique_freewayNames"]["buckets"]
    logging.debug("Aggregation results: %s", results)

    for item in results:
        # Modify the "key" field by replacing spaces with underscores
        item['key'] = item['key'].replace(' ', '_')

    
    # freeways = [result["key"] for result in results]
    # logging.info("Found freeways: %s", freeways)

    return {"freeways": results}

    


def aggregate_observations(es, freeway, year=None, month=None, day=None, hour=None):
    
    # freeway will be in the format of 'Monash_Fwy' change back to 'Monash Fwy'
    freeway = freeway.split('_')
    print("FreewayName is", freeway[0])
    
    # Define the query
    
    query = {
        "size": 0,  # We don't need any documents outside of our aggregations
        "query": {
            "bool": {
                "must": [
                    {"match": {"freewayName": freeway[0]}}
                ]
            }
        },
        "aggs": {
            "max_congestion": {
                "max": {
                    "field": "congestionIndex"
                }
            },
            "top_segments": {
                "terms": {
                    "field": "congestionIndex",
                    "size": 1,
                    "order": {
                        "max_congestion": "desc"
                    }
                },
                "aggs": {
                    "max_congestion": {
                        "max": {
                            "field": "congestionIndex"
                        }
                    },
                    "top_hit": {
                        "top_hits": {
                            "size": 1,
                            "_source": {
                                "includes": ["segmentName", "actualTravelTime", "geometry"]
                            }
                        }
                    }
                }
            }
        }
    }

        
    if month:
        try:
            month = int(month)
        except ValueError:
            return {"error": "month must be an integer"}
        
    if day:
        try:
            day = int(day)
        except ValueError:
            return {"error": "day must be an integer"}
    
    if hour:
        try:
            hour = int(hour)
        except ValueError:
            return {"error": "hour must be an integer"}
        
    if year:
        try:
            year = int(year)
            start_date, end_date = utils.get_date_limits(year, month, day, hour)
            query["query"]["bool"]["must"].append({
                "range": {
                    "publishedTime": {
                        "gte": start_date,
                        "lte": end_date,
                        "format": "yyyy-MM-dd'T'HH:mm:ss"
                    }
                }
            })
        except ValueError:
            return {"error": "year must be an integer"}
    


    print(query)

    logging.info("Executing Elasticsearch query...")
    # Execute the query
    res = es.search(index="traffic-data", body=query).body['aggregations']

    # print(res)

    logging.info("Query executed successfully")

    simplified_response = create_simplified_response(res)

    logging.info("Response generated successfully")

    return simplified_response