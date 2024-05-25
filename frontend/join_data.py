import requests
import geopandas as gpd
from shapely.geometry import LineString
import pandas as pd
from access_freeway_data import access_data
from haversine import haversine 

# Step 1: Load data from URLs
weather_stations = requests.get("http://localhost:9090/weather-stations").json()
air_quality_stations = requests.get("http://localhost:9090/air-quality-stations").json()
aggregated_traffic_data = access_data()

# Step 2: Load the shapefile for the SA2 regions
sf = gpd.read_file("data/SA2_2021_AUST_GDA2020.shp")
sf_vic = sf.loc[sf["STE_NAME21"] == "Victoria"]

# Step 3: Convert freeway coordinates to LineString
freeway_geometries = []
freeway_names = []
for segment, info in aggregated_traffic_data.items():
    if info['geometry_type'] == 'LineString':
        coordinates = info['coordinates']
        line = LineString(coordinates)
        freeway_geometries.append(line)
        freeway_names.append(segment)

# Create a GeoDataFrame for the freeway segments
freeway_gdf = gpd.GeoDataFrame({'Segment': freeway_names}, geometry=freeway_geometries)

# Step 4: Define function to find the closest SA2 for each freeway segment
def get_closest_sa2(freeway_line, sf_vic):
    min_distance = float('inf')
    closest_sa2 = None
    for idx, sa2_polygon in sf_vic.iterrows():
        distance = freeway_line.distance(sa2_polygon.geometry)
        if distance < min_distance:
            min_distance = distance
            closest_sa2 = sa2_polygon
    return closest_sa2

# Find the closest SA2 for each freeway segment
closest_sa2_list = []
for idx, freeway in freeway_gdf.iterrows():
    closest_sa2 = get_closest_sa2(freeway.geometry, sf_vic)
    closest_sa2_list.append(closest_sa2)

# Combine results
results = freeway_gdf.copy()
results['Closest_SA2_Code'] = [sa2['SA2_CODE21'] for sa2 in closest_sa2_list]
results['Closest_SA2_Name'] = [sa2['SA2_NAME21'] for sa2 in closest_sa2_list]
results['Closest_SA2_Lat'] = [sa2.geometry.centroid.y for sa2 in closest_sa2_list]
results['Closest_SA2_Lon'] = [sa2.geometry.centroid.x for sa2 in closest_sa2_list]

# Additional segments
princes_fwy_sa2 = sf_vic.loc[sf_vic["SA2_CODE21"] == "213051362"]
westgate_sa2 = sf_vic.loc[sf_vic["SA2_CODE21"] == "213041374"]

new_segments = [
    {
        "Segment": "Princes_Fwy",
        "geometry": None,
        "Closest_SA2_Code": princes_fwy_sa2.iloc[0]["SA2_CODE21"],
        "Closest_SA2_Name": princes_fwy_sa2.iloc[0]["SA2_NAME21"],
        "Closest_SA2_Lat": princes_fwy_sa2.iloc[0].geometry.centroid.y,
        "Closest_SA2_Lon": princes_fwy_sa2.iloc[0].geometry.centroid.x
    },
    {
        "Segment": "West_Gate_Fwy",
        "geometry": None,
        "Closest_SA2_Code": westgate_sa2.iloc[0]["SA2_CODE21"],
        "Closest_SA2_Name": westgate_sa2.iloc[0]["SA2_NAME21"],
        "Closest_SA2_Lat": westgate_sa2.iloc[0].geometry.centroid.y,
        "Closest_SA2_Lon": westgate_sa2.iloc[0].geometry.centroid.x
    }
]

# Append new segments to the results DataFrame
new_segments_df = pd.DataFrame(new_segments)
results = pd.concat([results, new_segments_df], ignore_index=True)


# Define the year for the API queries
year = 2024

# Function to find the closest station for each freeway segment
def add_closest_station_info(results, stations, station_type):
    # Prepare columns for the closest station information
    results[f'Closest_{station_type}_ID'] = None
    results[f'Closest_{station_type}_Name'] = None
    results[f'Closest_{station_type}_Distance_km'] = None

    # Iterate through each freeway segment
    for index, row in results.iterrows():
        freeway_coords = (row['Closest_SA2_Lat'], row['Closest_SA2_Lon'])
        closest_station = None
        min_distance = float('inf')
        
        # Iterate through each station to calculate the distance
        for station in stations['stations']:
            station_coords = (station['latitude'], station['longitude']) if 'latitude' in station else (station['lat'], station['lon'])
            distance = haversine(freeway_coords[0], freeway_coords[1], station_coords[0], station_coords[1])
            
            # Check if this is the closest station so far
            if distance < min_distance:
                min_distance = distance
                closest_station = station
        
        # Append the closest station information to the freeway DataFrame
        results.at[index, f'Closest_{station_type}_ID'] = closest_station['site_id'] if 'site_id' in closest_station else closest_station['wmo']
        results.at[index, f'Closest_{station_type}_Name'] = closest_station['site_name'] if 'site_name' in closest_station else closest_station['name']
        results.at[index, f'Closest_{station_type}_Distance_km'] = min_distance
    
    return results

# Add closest air quality station info
results = add_closest_station_info(results, air_quality_stations, 'Air_Quality')

# Add closest weather station info
results = add_closest_station_info(results, weather_stations, 'Weather')

# Function to fetch weather station data
def fetch_weather_data(weather_id, year):
    url = f"http://localhost:9090/weather-stations/{weather_id}?year={year}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if 'date_filter' in data:
            del data['date_filter']
        return data
    return {}

# Function to fetch air quality station data
def fetch_air_quality_data(air_quality_id, year):
    url = f"http://localhost:9090/air-quality-stations/{air_quality_id}?year={year}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if 'date_filter' in data:
            del data['date_filter']
        return data
    return {}

# Function to add station data to the DataFrame
def add_station_data(results, year):
    # Prepare columns for the weather and air quality data
    weather_columns = ['max_temperature', 'avg_wind_speed_kmh', 'max_wind_speed_kmh', 'min_wind_speed_kmh', 'avg_temperature', 'min_temperature']
    air_quality_columns = ['max_pm25', 'avg_pm25', 'min_pm25']
    
    for col in weather_columns + air_quality_columns:
        results[col] = None
    
    # Iterate through each row and fetch the corresponding station data
    for index, row in results.iterrows():
        if pd.notna(row['Closest_Weather_ID']):
            weather_data = fetch_weather_data(row['Closest_Weather_ID'], year)
            for col in weather_columns:
                if col in weather_data:
                    results.at[index, col] = weather_data[col]
        
        if pd.notna(row['Closest_Air_Quality_ID']):
            air_quality_data = fetch_air_quality_data(row['Closest_Air_Quality_ID'], year)
            for col in air_quality_columns:
                if col in air_quality_data:
                    results.at[index, col] = air_quality_data[col]
    
    return results

results = add_station_data(results, year)

# Load vehicle data and add to the DataFrame
vehicle_data = requests.get("http://localhost:9090/sudo-vehicle").json()

def add_vehicle_data(results, vehicle_data):
    vehicle_columns = ['Total_Dwellings', 'num_mot_veh_per_dwg_tot_dwgs']
    
    for col in vehicle_columns:
        results[col] = None
    
    for index, row in results.iterrows():
        SA2_code = str(row['Closest_SA2_Code'])
        if SA2_code in vehicle_data:
            vehicle_info = vehicle_data[SA2_code]
            for col in vehicle_columns:
                if col in vehicle_info:
                    results.at[index, col] = vehicle_info[col]
    
    return results

results = add_vehicle_data(results, vehicle_data)

# Save the results to a CSV file
results.to_csv("data/final_combined_df.csv", index=False)
