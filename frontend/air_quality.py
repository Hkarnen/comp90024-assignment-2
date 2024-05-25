import requests
import pandas as pd

def fetch_initial_air_quality_station_data():
    response = requests.get("http://localhost:9090/air-quality-stations")
    if response.status_code == 200:
        air_quality_stations_data = response.json()
        df_air_quality_stations = pd.DataFrame(air_quality_stations_data['stations'])

        # Define approximate latitude and longitude ranges for Melbourne
        melbourne_lat_range = (-38.1, -37.5)
        melbourne_lon_range = (144.5, 145.5)

        # Filter air quality stations in Melbourne
        melbourne_air_quality_stations = df_air_quality_stations[
            (df_air_quality_stations['latitude'] >= melbourne_lat_range[0]) & 
            (df_air_quality_stations['latitude'] <= melbourne_lat_range[1]) &
            (df_air_quality_stations['longitude'] >= melbourne_lon_range[0]) & 
            (df_air_quality_stations['longitude'] <= melbourne_lon_range[1])
        ]

        return melbourne_air_quality_stations
    else:
        print(f"Failed to retrieve air quality stations: {response.status_code}")
        print(response.text)
        return pd.DataFrame()

def fetch_detailed_air_quality_station_data(station_ids, station_names, year=2024, month=5):
    station_data_list = []

    for station_id, station_name in zip(station_ids, station_names):
        url = f"http://localhost:9090/air-quality-stations/{station_id}?year={year}&month={month}"
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            
            if 'date_filter' in data and 'start' in data['date_filter']:
                df_station_data = pd.DataFrame([data])
                df_station_data['date'] = pd.to_datetime(data['date_filter']['start'])
                df_station_data['station_id'] = station_id
                df_station_data['station_name'] = station_name
                station_data_list.append(df_station_data)
            else:
                print(f"'date_filter' or 'start' not found in data for {station_name} (Station ID: {station_id})")
        else:
            print(f"Failed to retrieve data for {station_name} (Station ID: {station_id}): {response.status_code}")
            print("Response content:")
            print(response.text)

    if station_data_list:
        df_stations_data = pd.concat(station_data_list, ignore_index=True)
        return df_stations_data
    else:
        print("No data retrieved for the specified stations.")
        return pd.DataFrame()

def fetch_air_quality_data():
    df_initial_stations = fetch_initial_air_quality_station_data()
    if df_initial_stations.empty:
        print("No initial air quality station data available.")
        return pd.DataFrame()

    station_ids = df_initial_stations['site_id'].tolist()
    station_names = df_initial_stations['site_name'].tolist()
    df_detailed_data = fetch_detailed_air_quality_station_data(station_ids, station_names)

    if df_detailed_data.empty:
        print("No detailed air quality data available.")
        return pd.DataFrame()

    # Merge the detailed data with initial data
    df_detailed_data = df_detailed_data.merge(df_initial_stations, left_on='station_id', right_on='site_id', how='left')
    return df_detailed_data

if __name__ == "__main__":
    df_stations_data = fetch_air_quality_data()
    if not df_stations_data.empty:
        print("Data fetched successfully.")
        print(df_stations_data.head())
    else:
        print("No data fetched.")