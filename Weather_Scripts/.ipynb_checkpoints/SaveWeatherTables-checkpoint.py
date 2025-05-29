import os
import pandas as pd

class Save_Weather_Tables:
    timestamp = ""
    
    def __init__(self, timestamp):
        self.timestamp = timestamp
        print("Initiate")

    def store_tables(self, df, filename):
        data = df.toPandas()
        directory = f"weather_tables/{self.timestamp}"
        os.makedirs(directory, exist_ok=True)

        parquetfilepath = os.path.join(directory, f"{filename}.parquet")
        data.to_parquet(parquetfilepath, engine='pyarrow') 
        csvfilepath = os.path.join(directory, f"{filename}.csv")
        data.to_csv(csvfilepath, index=False)
        jsonfilepath = os.path.join(directory, f"{filename}.json")
        data.to_json(jsonfilepath, orient='records', lines=True)