from datetime import datetime
import json
import os
import urllib.request

# --- AUTO-BOOTSTRAP WINDOWS HADOOP/WINUTILS TO PREVENT CRASHES ---
hadoop_bin = r"C:\hadoop\bin"
winutils_path = os.path.join(hadoop_bin, "winutils.exe")
hadoop_dll_path = os.path.join(hadoop_bin, "hadoop.dll")

if not os.path.exists(winutils_path):
  try:
    os.makedirs(hadoop_bin, exist_ok=True)
    print("Setting up local Hadoop environment binaries for Windows...")
    # Fetch compatible winutils and hadoop.dll binaries
    urllib.request.urlretrieve(
        "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.2.0/bin/winutils.exe",
        winutils_path,
    )
    urllib.request.urlretrieve(
        "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.2.0/bin/hadoop.dll",
        hadoop_dll_path,
    )
    print("Hadoop environment configured successfully!")
  except Exception as e:
    print(f"Notice during auto-setup: {e}")

os.environ["HADOOP_HOME"] = r"C:\\hadoop"
os.environ["hadoop.home.dir"] = r"C:\\hadoop"

import pandas as pd
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import arrays_zip, col, current_timestamp, explode, to_timestamp, when
from sqlalchemy import create_engine, text

# 1. Initialize PySpark Session
spark = (
    SparkSession.builder.appName("GlobalWeatherBigDataProject")
    .master("local[*]")
    .config(
        "spark.jars.packages", "com.microsoft.sqlserver:mssql-jdbc:12.2.0.jre8"
    )
    .getOrCreate()
)

# 2. Define Target Global & Indian Cities
cities = [
    {"city": "New York", "country": "USA", "lat": 40.7128, "lon": -74.0060},
    {"city": "Tokyo", "country": "Japan", "lat": 35.6762, "lon": 139.6503},
    {"city": "London", "country": "UK", "lat": 51.5074, "lon": -0.1278},
    {"city": "Sydney", "country": "Australia", "lat": -33.8688, "lon": 151.2093},
    {"city": "Paris", "country": "France", "lat": 48.8566, "lon": 2.3522},
    {"city": "New Delhi", "country": "India", "lat": 28.6139, "lon": 77.2090},
    {"city": "Mumbai", "country": "India", "lat": 18.9220, "lon": 72.8347},
    {"city": "Pune", "country": "India", "lat": 18.5204, "lon": 73.8567},
    {"city": "Bangalore", "country": "India", "lat": 12.9716, "lon": 77.5946},
]

raw_weather_payloads = []

print(
    "Fetching historical, live, and forecast weather data from Open-Meteo API..."
)
for loc in cities:
  url = (
      f"https://api.open-meteo.com/v1/forecast?"
      f"latitude={loc['lat']}&longitude={loc['lon']}"
      f"&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation"
      f"&past_days=30&forecast_days=10"
  )

  response = requests.get(url)
  if response.status_code == 200:
    data = response.json()
    data["city_name"] = loc["city"]
    data["country"] = loc["country"]
    raw_weather_payloads.append(data)

# 3. Load into PySpark DataFrame
json_rdd = spark.sparkContext.parallelize(
    [json.dumps(d) for d in raw_weather_payloads]
)
df_raw = spark.read.json(json_rdd)

print("Processing and flattening weather time-series data using PySpark...")
# 4. Flatten Nested Hourly Arrays
df_zipped = df_raw.withColumn(
    "weather_zipped",
    arrays_zip(
        col("hourly.time"),
        col("hourly.temperature_2m"),
        col("hourly.relative_humidity_2m"),
        col("hourly.wind_speed_10m"),
        col("hourly.precipitation"),
    ),
)

df_exploded = df_zipped.withColumn("w", explode(col("weather_zipped"))).select(
    col("city_name").alias("CityName"),
    col("country").alias("Country"),
    col("latitude").alias("Latitude"),
    col("longitude").alias("Longitude"),
    col("w.time").alias("timestamp_str"),
    col("w.temperature_2m").alias("Temperature"),
    col("w.relative_humidity_2m").alias("Humidity"),
    col("w.wind_speed_10m").alias("WindSpeed"),
    col("w.precipitation").alias("Precipitation"),
)

# 5. Format Timestamps & Tag Data (Actual/Live vs Forecast)
df_final = (
    df_exploded.withColumn(
        "Timestamp", to_timestamp(col("timestamp_str"), "yyyy-MM-dd'T'HH:mm")
    )
    .drop("timestamp_str")
    .withColumn(
        "Data_Type",
        when(
            col("Timestamp") <= current_timestamp(), "Actual / Live"
        ).otherwise("Forecast"),
    )
)

# Convert to Pandas for clean SQL loading integration
pandas_df = df_final.toPandas()

# 6. Connect to Local MSSQL Star Schema using Windows Authentication
connection_string = (
    "mssql+pyodbc://@localhost/Weather_Data_Project"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&trusted_connection=yes"
)
engine = create_engine(connection_string)

print("Writing clean dimensional and fact data to local MSSQL...")
with engine.begin() as conn:
  # Clear existing tables for fresh daily load
  conn.execute(text("DELETE FROM Fact_Weather_Metrics"))
  conn.execute(text("DELETE FROM Dim_Location"))
  conn.execute(text("DELETE FROM Dim_Date"))

# A. Populate Dim_Location
dim_location_df = (
    pandas_df[["CityName", "Country", "Latitude", "Longitude"]]
    .drop_duplicates()
    .reset_index(drop=True)
)
dim_location_df.to_sql(
    "Dim_Location", con=engine, if_exists="append", index=False
)

# Fetch LocationIDs back from SQL
loc_lookup = pd.read_sql(
    "SELECT LocationID, CityName FROM Dim_Location", con=engine
)

# B. Populate Dim_Date (Unique datetimes)
unique_times = pd.DataFrame(
    {"FullDateTime": pandas_df["Timestamp"].unique()}
)
unique_times["Year"] = unique_times["FullDateTime"].dt.year
unique_times["Month"] = unique_times["FullDateTime"].dt.month
unique_times["Day"] = unique_times["FullDateTime"].dt.day
unique_times["Hour"] = unique_times["FullDateTime"].dt.hour
unique_times["DayName"] = unique_times["FullDateTime"].dt.day_name()
unique_times.to_sql(
    "Dim_Date", con=engine, if_exists="append", index=False, chunksize=1000
)

# Fetch DateIDs back from SQL
date_lookup = pd.read_sql(
    "SELECT DateID, FullDateTime FROM Dim_Date", con=engine
)

# C. Merge keys into Fact Table DataFrame
fact_df = pandas_df.merge(
    loc_lookup, left_on="CityName", right_on="CityName", how="inner"
)
fact_df = fact_df.merge(
    date_lookup, left_on="Timestamp", right_on="FullDateTime", how="inner"
)

# Select final columns matching Fact_Weather_Metrics table
fact_final = fact_df[
    [
        "LocationID",
        "DateID",
        "Temperature",
        "Humidity",
        "WindSpeed",
        "Precipitation",
        "Data_Type",
    ]
]
fact_final.to_sql(
    "Fact_Weather_Metrics",
    con=engine,
    if_exists="append",
    index=False,
    chunksize=1000,
)

print(
    "Pipeline execution complete! Successfully populated Star Schema in MSSQL."
)