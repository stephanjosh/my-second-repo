import ee
import geopandas as gpd
import pandas as pd
import time


# --------------------------------------------
# Initialize Earth Engine
# --------------------------------------------
# ee.Initialize()

# Authenticate (run once to set up credentials)
ee.Authenticate()

# Initialize with your project ID
ee.Initialize(project='testapp-464712')

# # --------------------------------------------
# # Load Kenya Counties GeoJSON
# # --------------------------------------------
# counties = gpd.read_file("kenyan-counties.geojson")
# county_fc = ee.FeatureCollection(counties.__geo_interface__)

# # --------------------------------------------
# # Satellite Climate Datasets
# # --------------------------------------------

# # CHIRPS Daily Precipitation
# chirps = ee.ImageCollection("UCSB-CHG/CHIRPS/DAILY")

# # ERA5-Land Monthly Aggregates
# era5 = ee.ImageCollection("ECMWF/ERA5_LAND/MONTHLY")

# # --------------------------------------------
# # Function to extract monthly climate variables
# # --------------------------------------------
# def monthly_climate(year, month):

#     start = ee.Date.fromYMD(year, month, 1)
#     end = start.advance(1, "month")

#     # ---- Precipitation (CHIRPS)
#     precip = chirps.filterDate(start, end).sum().rename("precip_mm")

#     # ---- ERA5 variables
#     era_month = era5.filterDate(start, end).first()

#     # Convert Kelvin to Celsius
#     t_min = era_month.select("temperature_2m_min") \
#         .subtract(273.15).rename("temp_min_C")

#     t_max = era_month.select("temperature_2m_max") \
#         .subtract(273.15).rename("temp_max_C")

#     # Relative humidity (%)
#     humidity = era_month.select("relative_humidity_2m") \
#         .rename("humidity_percent")

#     # Combine all bands
#     combined = precip.addBands([
#         t_min,
#         t_max,
#         humidity
#     ]).set("year", year).set("month", month)

#     return combined


# # --------------------------------------------
# # Loop through 2014–2025
# # --------------------------------------------
# output = []

# for year in range(2014, 2026):
#     for month in range(1, 13):

#         print(f"Processing {year}-{month}")

#         img = monthly_climate(year, month)

#         # stats = img.reduceRegions(
#         #     collection=county_fc,
#         #     reducer=ee.Reducer.mean(),
#         #     scale=10000
#         # )

#         # geedoc = stats.getInfo()
#         # features = geedoc["features"]

#         # Reduce the data by only requesting what you need
#     # Get counties as a list
# county_list = county_fc.toList(county_fc.size())
# # total_counties = county_fc.size().getInfo()
# try:
#     # Get just the size - should be small
#     total_counties = county_fc.size().getInfo()
#     print(f"Total counties: {total_counties}")
# except ee.ee_exception.EEException as e:
#     print(f"Error getting county count: {e}")
#     print("Trying alternative method...")
    
#     # Alternative: Use a limit(1) to test
#     sample = county_fc.limit(1)
#     sample_size = sample.size().getInfo()
#     print(f"Sample size: {sample_size}")
    
#     # If that works, the full collection is too large
#     # You'll need to use a different approach
# # Process in small batches
# BATCH_SIZE = 3  # Start with 3 counties per batch
# num_batches = (total_counties + BATCH_SIZE - 1) // BATCH_SIZE

# for batch_num in range(num_batches):
#     start = batch_num * BATCH_SIZE
#     end = min((batch_num + 1) * BATCH_SIZE, total_counties)
    
#     print(f"Processing batch {batch_num + 1}/{num_batches} (counties {start}-{end})")
    
#     # Get this batch of counties
#     batch_list = county_list.slice(start, end)
#     batch_fc = ee.FeatureCollection(batch_list)
    
#     # Process this batch
#     batch_stats = img.reduceRegions(
#         collection=batch_fc,
#         reducer=ee.Reducer.mean(),
#         scale=10000
#     ).select(['mean'], retainGeometry=False)
    
#     # Get data for this batch
#     batch_geedoc = batch_stats.getInfo()  # ← This works because it's smaller
    
#     # ← YOUR ORIGINAL LOOP - KEEP THIS EXACTLY AS IS!
#     features = batch_geedoc["features"]
#     for f in features:
#         props = f["properties"]
#         props["year"] = year
#         props["month"] = month
#         output.append(props)


counties = gpd.read_file("kenyan-counties.geojson")
print("COUNTIES", counties)

# SIMPLIFY the geometries
print("Simplifying geometries...")
counties['geometry'] = counties.simplify(tolerance=0.01, preserve_topology=True)

# Reduce precision of coordinates
def round_coordinates(geom, decimals=4):
    if geom.is_empty:
        return geom
    return gpd.GeoSeries([geom]).round(decimals).iloc[0]

counties['geometry'] = counties['geometry'].apply(lambda x: round_coordinates(x, decimals=4))

print(f"Original CRS: {counties.crs}")
if counties.crs != 'EPSG:4326':
    counties = counties.to_crs('EPSG:4326')

# Convert to Earth Engine FeatureCollection
print("Converting to Earth Engine...")
county_fc = ee.FeatureCollection(counties.__geo_interface__)
print("THE COUNTY_FC", county_fc)

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

    # ERA5 variables - USING CORRECT BAND NAMES
    era_month = era5.filterDate(start, end).first()

    # Convert Kelvin to Celsius
    t_min = era_month.select("temperature_2m_min") \
        .subtract(273.15).rename("temp_min_C")

    t_max = era_month.select("temperature_2m_max") \
        .subtract(273.15).rename("temp_max_C")

    # DEWPOINT TEMPERATURE (use as proxy for humidity if needed)
    dewpoint = era_month.select("dewpoint_temperature_2m") \
        .subtract(273.15).rename("dewpoint_C")
    
    # Surface pressure (hPa)
    pressure = era_month.select("surface_pressure") \
        .multiply(0.01).rename("pressure_hPa")  # Convert Pa to hPa

    # Combine all bands
    combined = precip.addBands([t_min, t_max, dewpoint, pressure]) \
        .set("year", year).set("month", month)

    return combined

# --------------------------------------------
# Helper function to get county count safely
# --------------------------------------------
def get_county_count_safely(fc):
    """Try multiple methods to get county count"""
    try:
        count = fc.aggregate_count('.geo').getInfo()
        print(f"Count via aggregate_count: {count}")
        return count
    except:
        pass
    
    try:
        count = fc.limit(1000).size().getInfo()
        print(f"Count via limit(1000).size(): {count}")
        return count
    except:
        pass
    
    print("Using conservative estimate: 47 counties")
    return 47

# --------------------------------------------
# Process in small batches
# --------------------------------------------
output = []

# Get total counties safely
total_counties = get_county_count_safely(county_fc)
print(f"Total counties to process: {total_counties}")

# Get county list
county_list = county_fc.toList(total_counties)

# Process each year and month
for year in range(2014, 2026):
    for month in range(1, 13):
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
            
            try:
                batch_stats = img.reduceRegions(
                    collection=batch_fc,
                    reducer=ee.Reducer.mean(),
                    scale=10000
                )
                
                # Select only needed properties
                batch_stats = batch_stats.select(
                    ['precip_mm', 'temp_min_C', 'temp_max_C', 'dewpoint_C', 'pressure_hPa'], 
                    retainGeometry=False
                )
                
                # Get data
                batch_geedoc = batch_stats.getInfo()
                
                if 'features' in batch_geedoc:
                    features = batch_geedoc["features"]
                    
                    for f in features:
                        props = f["properties"]
                        props["year"] = year
                        props["month"] = month
                        
                        # Add county name if available
                        if 'properties' in f and 'COUNTY' in f['properties']:
                            props["county"] = f['properties']['COUNTY']
                        
                        output.append(props)
                    
                    print(f"  ✅ Added {len(features)} counties")
                            
            except Exception as e:
                print(f"  ❌ Batch failed: {e}")
                # Try individual counties
                print(f"    Processing individually...")
                for i in range(start, end):
                    try:
                        single_fc = ee.FeatureCollection([ee.Feature(county_list.get(i))])
                        single_stats = img.reduceRegions(
                            collection=single_fc,
                            reducer=ee.Reducer.mean(),
                            scale=10000
                        ).select(
                            ['precip_mm', 'temp_min_C', 'temp_max_C', 'dewpoint_C', 'pressure_hPa'],
                            retainGeometry=False
                        )
                        
                        single_geedoc = single_stats.getInfo()
                        if 'features' in single_geedoc and single_geedoc['features']:
                            props = single_geedoc['features'][0]["properties"]
                            props["year"] = year
                            props["month"] = month
                            output.append(props)
                            print(f"      ✅ County {i}")
                    except Exception as e2:
                        print(f"      ❌ County {i} failed: {e2}")
                    time.sleep(0.5)
            
            time.sleep(1)
# --------------------------------------------
# Save to CSV
# --------------------------------------------
df = pd.DataFrame(output)
df.to_csv("kenya_county_climate_2014_2025.csv", index=False)

print("Climate data extraction complete.")
