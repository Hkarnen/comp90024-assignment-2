import requests
import pandas as pd

def fetch_initial_weather_station_data():
    try:
        response = requests.get("http://localhost:9090/weather-stations")
        response.raise_for_status()
        stations_data = response.json()
        df_stations = pd.DataFrame(stations_data['stations'])

        # Define approximate latitude and longitude ranges for Melbourne
        melbourne_lat_range = (-38.1, -37.5)
        melbourne_lon_range = (144.5, 145.5)

        # Filter stations in Melbourne
        melbourne_stations = df_stations[
            (df_stations['lat'] >= melbourne_lat_range[0]) & 
            (df_stations['lat'] <= melbourne_lat_range[1]) &
            (df_stations['lon'] >= melbourne_lon_range[0]) & 
            (df_stations['lon'] <= melbourne_lon_range[1])
        ]

        return melbourne_stations
    except requests.exceptions.RequestException as e:
        print(f"Failed to retrieve weather stations: {e}")
        print(f"Response content: {response.text}")
        return pd.DataFrame()

def fetch_detailed_weather_station_data(station_ids, year=2024, month=5):
    station_data_list = []

    for station_id in station_ids:
        url = f"http://localhost:9090/weather-stations/{station_id}?year={year}&month={month}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            station_data = response.json()
            df_station_data = pd.DataFrame(station_data)
            df_station_data['station_id'] = station_id
            station_data_list.append(df_station_data)
        except requests.exceptions.RequestException as e:
            print(f"Failed to retrieve data for station {station_id}: {e}")
            print(f"Response content: {response.text}")

    if station_data_list:
        df_stations_data = pd.concat(station_data_list, ignore_index=True)
        return df_stations_data
    else:
        print("No data retrieved for the specified stations.")
        return pd.DataFrame()

def fetch_weather_station_data():
    df_initial_stations = fetch_initial_weather_station_data()
    if df_initial_stations.empty:
        print("No initial weather station data available.")
        return pd.DataFrame()

    station_ids = df_initial_stations['wmo'].tolist()
    df_detailed_data = fetch_detailed_weather_station_data(station_ids)

    if df_detailed_data.empty:
        print("No detailed weather data available.")
        return pd.DataFrame()

    # Merge the detailed data with initial data
    df_detailed_data = df_detailed_data.merge(df_initial_stations, left_on='station_id', right_on='wmo', how='left')
    return df_detailed_data

if __name__ == "__main__":
    df_stations_data = fetch_weather_station_data()
    if not df_stations_data.empty:
        print("Data fetched successfully.")
        print(df_stations_data.head())
    else:
        print("No data fetched.")
