import ee
import geopandas as gpd
import pandas as pd
import time 


# Authenticate (run once to set up credentials)
ee.Authenticate()

# Initialize with your project ID
ee.Initialize(project='testapp-464712')

# Load counties from GeoJSON
counties = gpd.read_file("kenyan-counties.geojson")
# here I have an empty list which I want to feed in with the counties name
print("COUNTIES columns:", counties.columns.tolist())
print("First county:", counties.iloc[0].to_dict())

# SIMPLIFY the geometries
print("Simplifying geometries...")
counties['geometry'] = counties.simplify(tolerance=0.01, preserve_topology=True)

# Reduce precision of coordinates -- to reduce the datasize and avoid issues with GEE
def round_coordinates(geom, decimals=4):
    if geom.is_empty:
        return geom
    return gpd.GeoSeries([geom]).round(decimals).iloc[0]

counties['geometry'] = counties['geometry'].apply(lambda x: round_coordinates(x, decimals=4))
# stored as coordinates
print(f"Original CRS: {counties.crs}")
if counties.crs != 'EPSG:4326':
    counties = counties.to_crs('EPSG:4326')

# Find the county name column
possible_name_columns = ['COUNTY', 'NAME', 'county', 'ADM1_EN', 'ADM1_NAME', 'DISTRICT', 'Constituency']
county_name_col = None

# Simple for loop to find the first column that looks like it contains county names. We check common name fields first, then fall back to any string column if needed. This way we can handle different datasets with varying schemas.
for col in possible_name_columns:
    if col in counties.columns:
        county_name_col = col
        break

if county_name_col is None:
    for col in counties.columns:
        if counties[col].dtype == 'object' and col != 'geometry':
            county_name_col = col
            break

print(f"Using county name column: {county_name_col}")

# Ensure county names are strings and clean
counties[county_name_col] = counties[county_name_col].astype(str).str.strip()

# Create a backup of county names
county_names_list = counties[county_name_col].tolist()
print(f"County names: {county_names_list[:5]}...")

# Convert to Earth Engine FeatureCollection
print("Converting to Earth Engine...")
geo_json = counties.__geo_interface__
county_fc = ee.FeatureCollection(geo_json)

# --------------------------------------------
# Satellite Climate Datasets
# --------------------------------------------
chirps = ee.ImageCollection("UCSB-CHG/CHIRPS/DAILY")
era5 = ee.ImageCollection("ECMWF/ERA5_LAND/MONTHLY_AGGR")

# --------------------------------------------
# Function to extract monthly climate variables
# --------------------------------------------
def monthly_climate(year, month):
    start = ee.Date.fromYMD(year, month, 1)
    end = start.advance(1, "month")


    # Precipitation (CHIRPS)
    precip = chirps.filterDate(start, end).sum().rename("precip_mm")

    # ERA5 variables
    era_month = era5.filterDate(start, end).first()

    # Convert Kelvin to Celsius
    t_min = era_month.select("temperature_2m_min") \
        .subtract(273.15).rename("temp_min_C")

    t_max = era_month.select("temperature_2m_max") \
        .subtract(273.15).rename("temp_max_C")

    # Dewpoint temperature
    dewpoint = era_month.select("dewpoint_temperature_2m") \
        .subtract(273.15).rename("dewpoint_C")
    
    # Surface pressure (hPa)
    pressure = era_month.select("surface_pressure") \
        .multiply(0.01).rename("pressure_hPa")
    
    # Surface solar radiation downwards (daily/monthly sum)
    # ✅ CORRECT (convert J/m² to MJ/m²):
    radiation = (
        era_month.select("surface_solar_radiation_downwards_sum")
            .divide(1_000_000)
            .divide(30)  # Assuming 30 days in a month for average daily radiation
            .rename("solar_rad_MJ_m2_day")
    )
    
    
    # Combine U and V components into wind speed
    wind_u = era_month.select("u_component_of_wind_10m");
    wind_v = era_month.select("v_component_of_wind_10m");
    wind_speed = wind_u.pow(2).add(wind_v.pow(2)).sqrt() \
    .rename("wind_speed_10m_ms");

    runoff = era_month.select([
    "surface_runoff_sum",
    "sub_surface_runoff_sum"
]).reduce(ee.Reducer.sum()).multiply(1000).rename("runoff")

    # there is some missing data --- soil_moisture, evap_bare_soil, evap_transpiration, evap_open_water, solar_rad_MJ_m2_day


    soil_moisture = era_month.select([
    "volumetric_soil_water_layer_1",
    "volumetric_soil_water_layer_2",
    "volumetric_soil_water_layer_3",
    "volumetric_soil_water_layer_4"
    ]).rename([
        "soil_moisture_layer1", "soil_moisture_layer2",
        "soil_moisture_layer3", "soil_moisture_layer4"
    ])   

    # Unmask soil moisture with a sentinel value
    soil_moisture_unmasked = soil_moisture.unmask(-9999);

    # ✅ Correct assignment (swap the values)
    evap_bare_soil = era_month.select("evaporation_from_bare_soil_sum") \
    .multiply(-1) \
    .rename("evap_bare_soil")

    evap_transpiration = era_month.select("evaporation_from_vegetation_transpiration_sum") \
        .multiply(-1) \
        .rename("evap_transpiration")

    evap_open_water = era_month.select("evaporation_from_open_water_surfaces_excluding_oceans_sum") \
        .multiply(-1) \
    .rename("evap_open_water")

    # Combine all bands
    combined = precip.addBands([t_min, t_max, dewpoint, pressure, soil_moisture_unmasked, evap_bare_soil, evap_transpiration,evap_open_water, radiation, wind_speed, runoff]) \
        .set("year", year).set("month", month)

    return combined

# --------------------------------------------
# Process in small batches
# --------------------------------------------
output = []

# Get total counties
total_counties = county_fc.size().getInfo()
print(f"Total counties to process: {total_counties}")

# Get county list
county_list = county_fc.toList(total_counties)

for year in range(2014, 2026):  
    for month in range(1, 2):
        print(f"\n{'='*50}")
        print(f"Processing {year}-{month}")
        print(f"{'='*50}")
        
        img = monthly_climate(year, month)
        
        # Process in small batches
        BATCH_SIZE = 2
        num_batches = (total_counties + BATCH_SIZE - 1) // BATCH_SIZE
        
        for batch_num in range(num_batches):
            start = batch_num * BATCH_SIZE
            end = min((batch_num + 1) * BATCH_SIZE, total_counties)
            
            print(f"\n📦 Batch {batch_num + 1}/{num_batches} (counties {start}-{end})")
            
            # Get this batch of counties
            batch_list = county_list.slice(start, end)
            batch_fc = ee.FeatureCollection(batch_list)

            # had to predefine the era_month and scale here to avoid the error about missing propertie
            era_month = era5.filterDate(start, end).first()
            # scale = era_month.projection().nominalScale(); 

            try:
                # CRITICAL FIX: Don't select only climate variables - keep all properties
                batch_stats = img.reduceRegions(
                    collection=batch_fc,
                    reducer=ee.Reducer.mean(),
                    scale=1000
                )
                
                # Get data with ALL properties
                batch_geedoc = batch_stats.getInfo()
                
                if 'features' in batch_geedoc:
                    features = batch_geedoc["features"]
                    for f in features:
                        props = f["properties"]
                        
                        # Extract climate data
                        climate_record = {
                            "year": year,
                            "month": month,
                            "precip_mm": props.get("precip_mm"),
                            "temp_min_C": props.get("temp_min_C"),
                            "temp_max_C": props.get("temp_max_C"),
                            "dewpoint_C": props.get("dewpoint_C"),
                            "pressure_hPa": props.get("pressure_hPa"),
                            "soil_moisture_layer1": props.get("soil_moisture_layer1"),
                            "soil_moisture_layer2": props.get("soil_moisture_layer2"),
                            "soil_moisture_layer3": props.get("soil_moisture_layer3"),
                            "soil_moisture_layer4": props.get("soil_moisture_layer4"),
                            "evap_bare_soil": props.get("evap_bare_soil"),
                            "evap_transpiration": props.get("evap_transpiration"),
                            "evap_open_water": props.get("evap_open_water"),
                            "solar_rad_MJ_m2_day": props.get("solar_rad_MJ_m2_day"),
                            "wind_speed_10m_ms": props.get("wind_speed_10m_ms"),
                            "runoff": props.get("runoff"),
                        }
                        
                        # Get county name - try multiple sources
                        county_name = None
                        
                        # Try the column we identified
                        if county_name_col and county_name_col in props:
                            county_name = props[county_name_col]
                        
                        # Try common name fields
                        if county_name is None:
                            for name_field in ['COUNTY', 'NAME', 'county', 'ADM1_EN']:
                                if name_field in props:
                                    county_name = props[name_field]
                                    break
                        
                        # Use index as fallback
                        if county_name is None:
                            idx = start + (features.index(f) if features.index(f) < len(features) else 0)
                            county_name = county_names_list[idx] if idx < len(county_names_list) else f"County_{idx}"
                        
                        climate_record["county"] = county_name
                        output.append(climate_record)
                    
                    print(f"  ✅ Added {len(features)} counties")
                            
            except Exception as e:
                print(f"  ❌ Batch failed: {e}")
                # Individual counties
                print(f"    Processing individually...")
                for i in range(start, end):
                    try:
                        county_name = county_names_list[i] if i < len(county_names_list) else f"County_{i}"
                        
                        single_fc = ee.FeatureCollection([ee.Feature(county_list.get(i))])
                        single_stats = img.reduceRegions(
                            collection=single_fc,
                            reducer=ee.Reducer.mean(),
                            scale=10000
                        )
                        
                        single_geedoc = single_stats.getInfo()
                        if 'features' in single_geedoc and single_geedoc['features']:
                            props = single_geedoc['features'][0]["properties"]
                            
                            climate_record = {
                                "year": year,
                                "month": month,
                                "county": county_name,
                                "precip_mm": props.get("precip_mm"),
                                "temp_min_C": props.get("temp_min_C"),
                                "temp_max_C": props.get("temp_max_C"),
                                "dewpoint_C": props.get("dewpoint_C"),
                                "pressure_hPa": props.get("pressure_hPa"),
                                "soil_moisture_layer1": props.get("soil_moisture_layer1"),
                                "soil_moisture_layer2": props.get("soil_moisture_layer2"),
                                "soil_moisture_layer3": props.get("soil_moisture_layer3"),
                                "soil_moisture_layer4": props.get("soil_moisture_layer4"),
                                "evap_bare_soil": props.get("evap_bare_soil"),
                                "evap_transpiration": props.get("evap_transpiration"),
                                "evap_open_water": props.get("evap_open_water"),
                                "solar_rad_MJ_m2_day": props.get("solar_rad_MJ_m2_day"),
                                "wind_speed_10m_ms": props.get("wind_speed_10m_ms"),
                                "runoff": props.get("runoff"),
                            }
                            output.append(climate_record)
                            print(f"      ✅ County {i}: {county_name}")
                    except Exception as e2:
                        print(f"      ❌ County {i} failed: {e2}")
                    time.sleep(0.5)
            
            time.sleep(1)

# --------------------------------------------
# Save to CSV
# --------------------------------------------
df = pd.DataFrame(output)

# Reorder columns to put county first
cols = ['county', 'year', 'month', 'precip_mm', 'temp_min_C', 'temp_max_C', 'dewpoint_C', 'pressure_hPa', 'soil_moisture_layer1', 'soil_moisture_layer2', 'soil_moisture_layer3', 'soil_moisture_layer4', 'evap_bare_soil', 'evap_transpiration', 'evap_open_water', 'solar_rad_MJ_m2_day', 'wind_speed_10m_ms', 'runoff']
df = df[cols]

# Sort by county, year, month
df = df.sort_values(['county', 'year', 'month'])

# Coastal counties may have unrealistic values - set unrealistic negatives to a reasonable value like 0.15
df.loc[df['soil_moisture_layer1'] < 0, 'soil_moisture_layer1'] = 0.15
df.loc[df['soil_moisture_layer2'] < 0, 'soil_moisture_layer2'] = 0.15
df.loc[df['soil_moisture_layer3'] < 0, 'soil_moisture_layer3'] = 0.15
df.loc[df['soil_moisture_layer4'] < 0, 'soil_moisture_layer4'] = 0.15


# Save to CSV
df.to_csv("kenya_county_climate_2014_2025.csv", index=False)

print("\n" + "="*50)
print("✅ Climate data extraction complete!")
print("="*50)
print(f"Total records: {len(df)}")
print(f"Unique counties: {df['county'].nunique()}")
print(f"Years: {df['year'].min()} - {df['year'].max()}")
print("\nFirst few rows:")
print(df.head(10))
print(f"\nFile saved as: final_kenya_county_climate_2014_2025.csv")