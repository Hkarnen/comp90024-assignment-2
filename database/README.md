# ElasticSearch Type Mappings

## Traffic index type mappings
- obs_id: keyword 
- freewayName: text
- segmentName: text
- publishedTime: date, of format yyyy-mm--dd'T'HH:mm:ss
- condition: text
- actualTravelTime: integer
- averageSpeed: integer
- congestionIndex: double
- geometry: geo_shape
