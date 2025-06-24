#!/usr/bin/env python
# coding: utf-8

# In[30]:

import boto3
import requests
import urllib.request
import httpx
import os
import pandas as pd
import pyspark
import json
import pyarrow
import logging

from GetWeather import Import_Weather_Data
from SetupWeatherData import Setup_Weather_Data
from Upload_Weather_Data import UploadWeatherData
from ExportWeatherData import Export_Weather_Data
from SaveWeatherTables import Save_Weather_Tables

from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql import Row
from pyspark.sql.window import Window
from py4j.protocol import Py4JJavaError
from datetime import datetime, timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, BooleanType


# In[31]:

def main():
    logging.basicConfig(
        level = logging.INFO,
        format= '%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    
    # In[32]:
    
    
    # Hello Github
    
    
    # In[33]:
    
    
    # Load my .env files for API keys
    
    # Get Current Directory
    current_directory = os.getcwd()
    
    # Load my scripts for API keys
    #base_dir = os.path.abspath(os.path.join(current_directory, "../../sensitive_data"))
    #print(base_dir)
    base_dir = "/home/cephuez/sensitive_data"
    
    weather_env_path = os.path.join(base_dir, "weather_api.env")
    aws_env_path = os.path.join(base_dir, "aws_info.env")
    google_env_path = os.path.join(base_dir, "google_info.env")
    azure_env_path = os.path.join(base_dir, "azure_info.env")
    
    load_dotenv(dotenv_path=weather_env_path)
    load_dotenv(dotenv_path=aws_env_path)
    load_dotenv(dotenv_path=google_env_path)
    load_dotenv(dotenv_path=azure_env_path)
    
    
    # In[34]:
    
    
    spark = SparkSession.builder.appName("Weather_Session").getOrCreate()
    
    
    # In[35]:
    
    
    timestamp = datetime.now().strftime("%Y-%B-%d_%H-%M")
    weather_data = Import_Weather_Data(timestamp)
    filename = weather_data.get_filename()
    
    
    # In[36]:
    
    
    setup_weather_data = Setup_Weather_Data(spark, filename, timestamp)
    df = setup_weather_data.get_data_frame()
    
    
    # In[37]:
    
    
    weather_result_filename = setup_weather_data.get_weather_result_filename()
    logger.info(f"Output folder: {weather_result_filename}")
    
    
    # In[38]:
    
    
    #df.show()
    
    
    # In[39]:
    
    
    export_data = Export_Weather_Data(weather_result_filename)
    
    
    # In[40]:
    
    
    save_tables = Save_Weather_Tables(timestamp)
    
    
    # In[41]:
    
    
    # Create location DFx
    location_df = df.select(
        df["id"].alias("ID"),
        df["name"].alias("City"),
        df["sys.country"].alias("Country"),
        df["coord.lat"].alias("Latitude"),
        df["coord.lon"].alias("Longitude")
    ).orderBy("ID")
    
    save_tables.store_tables(location_df, "Location_Table")
    
    
    # In[42]:
    
    
    # Create Temperature & Pressure Table
    temperature_df = df.select(
        df["id"].alias("City_ID"),
        df["main.temp"].alias("Temp"),
        df["main.temp_max"].alias("Temp_Max"),
        df["main.temp_min"].alias("Temp_Min"),
        df["main.feels_like"].alias("Feels_Like"),
        df["main.humidity"].alias("Humidity"),
        df["main.pressure"].alias("Pressure"),
        df["main.sea_level"].alias("Sea_Level")
    ).orderBy("City_ID")
    
    save_tables.store_tables(temperature_df, "Temperature_Table")
    
    
    # In[43]:
    
    
    # Create Wind & Clouds Table
    wind_df = df.select(
        df["id"].alias("City_ID"),
        df["clouds.all"].alias("Cloudiness_Percentage"),
        df["wind.deg"].alias("Wind_Direction_Degree"),
        df["wind.gust"].alias("Gust_Speed"),
        df["wind.speed"].alias("Wind_Speed")
    ).orderBy("City_ID")
    
    save_tables.store_tables(wind_df, "Wind_Cloud_Table")
    
    
    # In[44]:
    
    
    # Create Weather Description
    weather_desc_df = df.select(
        df["id"].alias("City_ID"),
        df["weather"][0]["main"].alias("Main_Weather"),
        df["weather"][0]["description"].alias("Description"),
        df["weather"][0]["icon"].alias("Icon")
    ).orderBy("City_ID")
    
    save_tables.store_tables(weather_desc_df, "Weather_Description_Table")
    
    
    # In[45]:
    
    
    # Sunrise_Sunset_Table
    sunrise_sunset_df = df.select(
        df["id"].alias("City_ID"),
        df["sys.sunrise"].alias("Sunrise"),
        df["sys.sunset"].alias("Sunset"),
        df["timezone"].alias("Timezone")
    ).orderBy("City_ID")
    save_tables.store_tables(sunrise_sunset_df, "Sunrise_Sunset_Table")
    
    
    # In[46]:
    
    
    # Save current tables
    # Save a copy of the tables into the cloud service
    save_tables.store_into_cloud()
    
    
    # In[47]:
    
    
    # Which cities have the longest daylight duration?
    # Convert the time into readable time. Order by daylight hour
    top_10_cities = sunrise_sunset_df.select(
        col("City_ID"), 
        date_format(from_unixtime(col("Sunrise") + col("Timezone")),"HH:mm:ss").alias("Sunrise"),
        date_format(from_unixtime(col("Sunset") + col("Timezone")),"HH:mm:ss").alias("Sunset"), 
        round(((col("Sunset") - col("Sunrise")) / 3600),2).alias("Daylight_Hours"),
        col("Timezone")).orderBy(col("Daylight_Hours").desc()).limit(10)
    
    final_table = top_10_cities.join(location_df, top_10_cities["City_ID"] == location_df["ID"]
                    ).select(location_df["ID"], 
                             location_df["City"], 
                             location_df["Country"], 
                             top_10_cities["Sunrise"], 
                             top_10_cities["Sunset"], 
                             top_10_cities["Daylight_Hours"], 
                             top_10_cities["Timezone"]
                    ).orderBy(col("Daylight_Hours").desc())
    #final_table.show()
    
    final_table.createOrReplaceTempView("Final_Table")
    
    query = '''
            SELECT ID, CITY, COUNTRY, SUNRISE, SUNSET, DAYLIGHT_HOURS, TIMEZONE, RANK()OVER(ORDER BY DAYLIGHT_HOURS DESC) RANK
            FROM FINAL_TABLE
        '''
    result = spark.sql(query)
    #result.show()
    
    
    data = result.toPandas()
    
    export_data.to_parquet(data, 'Longest_Daytime.parquet')
    export_data.to_csv(data, 'Longest_Daytime.csv')
    export_data.to_json(data, 'Longest_Daytime.json')
    
    
    # In[48]:
    
    
    # Which city has the highest difference between actual temperature and feels-like temperature?
    # temperature_df
    top_10_cities = temperature_df.select(
        col("City_ID"), 
        col("Temp"), 
        col("Feels_Like"), 
        round(abs(col("Temp") - col("Feels_Like")),2).alias("Difference")
            ).orderBy(col("Difference").desc()).limit(10)
    #top_10_cities.show()
    
    final_top_10_cities_temperature = top_10_cities.join(location_df, top_10_cities["City_ID"] == location_df["ID"]).select(
        top_10_cities["City_ID"], 
        location_df["City"], 
        location_df["Country"], 
        top_10_cities["Temp"], 
        top_10_cities["Feels_Like"], 
        top_10_cities["Difference"]
            ).orderBy(col("Difference").desc())
    #final_top_10_cities_temperature.show()
    
    final_top_10_cities_temperature.createOrReplaceTempView("Final_Table")
    
    query = '''
            SELECT CITY_ID, CITY, COUNTRY, TEMP, FEELS_LIKE, DIFFERENCE, RANK()OVER(ORDER BY DIFFERENCE DESC) RANK
            FROM FINAL_TABLE
        '''
    
    final_result = spark.sql(query)
    #final_result.show()
    
    data = final_result.toPandas()
    
    export_data.to_parquet(data, 'Temperature_Feel_Like_Temperature_Diff.parquet')
    export_data.to_csv(data, 'Temperature_Feel_Like_Temperature_Diff.csv')
    export_data.to_json(data, 'Temperature_Feel_Like_Temperature_Diff.json')
    
    weather_uploader = UploadWeatherData(weather_result_filename)
    
    # Upload to AWS
    weather_uploader.upload_to_AWS()
    
    weather_uploader.upload_to_Google()
    
    weather_uploader.upload_to_Azure()



