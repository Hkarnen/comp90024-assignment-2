# script to add site id and and site name to existing records in the air_quality_data index
import elasticsearch
import epa

es = elasticsearch.Elasticsearch(
            'https://localhost:9200',
            verify_certs=False,
            http_auth=('elastic', 'elastic')
)

query = {
  "query": {
    "bool": {
      "must_not": [
        {
          "exists": {
            "field": "site_id"
          }
        },
        {
          "exists": {
            "field": "site_name"
          }
        }
      ]
    }
  }
}

sites = epa.get_sites()["records"]
sites

site_dict = {site["siteID"]: site["siteName"] for site in sites}
site_dict

res = es.search(index="air_quality_data", body=query)

error_obs = []

for record in res.body["hits"]["hits"]:
    print(record["_id"])
    site_id = record["_id"][0:36]
    data = record["_source"]
    if site_id not in site_dict:
        error_obs.append(record["_id"])
    else:
        data["site_name"] = site_dict[site_id]
        data["site_id"] = site_id
        es.index(index="air_quality_data", id=record["_id"], body=data)
        
error_obs
        