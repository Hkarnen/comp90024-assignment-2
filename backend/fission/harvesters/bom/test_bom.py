import unittest
from unittest.mock import patch
import pandas as pd
import json

import bom

class TestBomHarvester(unittest.TestCase):
    def test_parse_stations_table(self):
        with open('tests/stations.txt', 'rb') as f:
            raw_stations_table = f.read()
            
        stations = bom.parse_stations_table(raw_stations_table)
        
        expect_stations = pd.DataFrame({
            "WMO": ["89570", "89809", "89817", "99963"],
            "Site name": ["DAVIS (WHOOP WHOOP)", "CASEY SKIWAY SOUTH", "BUNGER HILLS", "BROWNING PENINSULA"],
            "Lat": ["-68.4723", "-66.2803", "-66.2510", "-66.4870"],
            "Lon": ["78.8735", "110.7615", "100.6000", "110.5690"]
        })
        expect_stations.set_index("WMO", inplace=True)
        
        self.assertTrue((stations == expect_stations).all().all())
                
    
    @patch("bom.requests.get")
    def test_get_vic_weather_stations(self, mock_get):
        mock_get.return_value.raise_for_status.return_value = None
        with open('tests/vic_stations.html', 'r') as f:
            mock_get.return_value.text = f.read()
        
        stations = bom.get_vic_weather_stations_urls(bom.VIC_WEATHER_STATIONS)
        with open('tests/expected_vic_stations.txt', 'r') as f:
            expected_stations = f.readlines()
        
        # no whitespace in the html document
        expected_stations = [station.strip() for station in expected_stations]

        self.assertEqual(stations, expected_stations)
        
    @patch("bom.requests.get")
    def test_get_weather_data(self, mock_get):
        mock_get.return_value.raise_for_status.return_value = None
        with open('tests/weather_data.json', 'r') as f:
           mock_get.return_value.json.return_value = json.load(f)

        weather_data = bom.get_weather_data("99963")
        expected_data = [
            {
                "sort_order": 0,
                "wmo": 94839,
                "name": "Charlton",
                "history_product": "IDV60801",
                "local_date_time": "04/03:00pm",
                "local_date_time_full": "20240504150000",
                "aifstime_utc": "20240504050000",
                "lat": -36.3,
                "lon": 143.3,
                "apparent_t": 15.8,
                "cloud": "-",
                "cloud_base_m": None,
                "cloud_oktas": None,
                "cloud_type": "-",
                "cloud_type_id": None,
                "delta_t": 7.2,
                "gust_kmh": 24,
                "gust_kt": 13,
                "air_temp": 20.1,
                "dewpt": 5.4,
                "press": 1022.5,
                "press_msl": 1022.5,
                "press_qnh": 1022.4,
                "press_tend": "-",
                "rain_trace": "0.0",
                "rel_hum": 38,
                "sea_state": "-",
                "swell_dir_worded": "-",
                "swell_height": None,
                "swell_period": None,
                "vis_km": "-",
                "weather": "-",
                "wind_dir": "ESE",
                "wind_spd_kmh": 17,
                "wind_spd_kt": 9
            }
        ]
        
        self.assertEqual(weather_data, expected_data)

if __name__ == "__main__":
    unittest.main()