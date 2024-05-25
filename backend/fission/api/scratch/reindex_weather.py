import elasticsearch
import json

es = elasticsearch.Elasticsearch(
    "https://localhost:9200",
    http_auth=("elastic", "elastic"),
    verify_certs=False
)

mapping = json.load(open("/Users/lachlanhugo/Documents/uni/MDataScience/2024/COMP90024/comp90024-assignment-2/backend/fission/harvesters/bom/index_remapping.json"))

mapping

es.indices.create(index="new_weather_data", body=mapping)

es.indices.delete(index="new_weather_data")

resp = es.reindex(
    body={
        "source": {"index": "weather_data"},
        "dest": {"index": "new_weather_data"}
    },
    wait_for_completion=False
)

resp.body

es.search(index="new_weather_data")
