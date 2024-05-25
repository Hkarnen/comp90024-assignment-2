import unittest
from unittest.mock import patch, MagicMock
import backend.fission.harvesters.traffic.traffic_harvester as traffic_harvester

class TestTrafficHarvester(unittest.TestCase):
    @patch('traffic_harvester.Elasticsearch')
    @patch('traffic_harvester.requests.get')
    def test_fetch_and_process_traffic_data(self, mock_es_client, mock_get):
        # Setup Elasticsearch mock
        mock_es = MagicMock()
        mock_es.exists.return_value = False  # Assume no data exists initially
        mock_es_client.return_value = mock_es
        
        # Setup the mock response for API call
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "features": [
                {
                    "properties": {
                        "obs_id": "1---2023-04-25T12:00:00",
                        "freewayName": "Freeway 1",
                        "segmentName": "Segment A",
                        "publishedTime": "2023-04-25T12:00:00",
                        "condition": "Good",
                        "actualTravelTime": 30,
                        "averageSpeed": 70,
                        "congestionIndex": 0.5,
                        "geometry": {"type": "LineString", "coordinates": [[144.9631, -37.8136], [144.9641, -37.8146]]}
                    }
                },
            ]
        }

        # Execute the function
        results = traffic_harvester.fetch_and_process_traffic_data()

        # Assertions
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['obs_id'], '1---2023-04-25T12:00:00')
        self.assertEqual(results[0]['freewayName'], 'Freeway 1')
        self.assertEqual(mock_es.index.call_count, 1)  # Check that indexing was called once

        # Check if the existing data check was performed
        mock_es.exists.assert_called_once_with(index="traffic-data", id='1---2023-04-25T12:00:00')


    @patch('traffic_harvester.requests.get')
    def test_fetch_data_failure(self, mock_get):
        # Setup mock
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        # Test
        result = traffic_harvester.fetch_and_process_traffic_data()
        self.assertIsNone(result)

if __name__ == '__main__':
    unittest.main()
