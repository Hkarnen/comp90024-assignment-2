import requests

def access_data():
    response = requests.get("http://localhost:9090/traffic-freeway")
    freeways_json = response.json()

    freeways = []

    for entry in freeways_json["freeways"]:
        freeways.append(entry["key"])

    aggregated_data = {}

    for freeway in freeways:
        response = requests.get(f"http://localhost:9090/traffic-freeway/{freeway}?year=2024&month=5")
        freeway_data = response.json()
        # print(freeway, response)
        aggregated_data[freeway] = freeway_data

    return aggregated_data

aggregated_data = access_data()

print(aggregated_data)