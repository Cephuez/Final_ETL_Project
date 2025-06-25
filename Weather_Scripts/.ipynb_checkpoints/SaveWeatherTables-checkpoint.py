import os
import boto3
import pandas as pd
from google.cloud import storage
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

class Save_Weather_Tables:
    timestamp = ""
    directory = ""
    google_json_name = "/Google_Cloud_Key.json"
    
    def __init__(self, timestamp):
        self.timestamp = timestamp

    def store_tables(self, df, filename):
        data = df.toPandas()
        directory = f"weather_tables/{self.timestamp}"
        self.directory = directory
        os.makedirs(directory, exist_ok=True)
        print("Make Directory")

        parquetfilepath = os.path.join(directory, f"{filename}.parquet")
        data.to_parquet(parquetfilepath, engine='pyarrow') 
        
        csvfilepath = os.path.join(directory, f"{filename}.csv")
        data.to_csv(csvfilepath, index=False)
        
        jsonfilepath = os.path.join(directory, f"{filename}.json")
        data.to_json(jsonfilepath, orient='records', lines=True)

    def store_into_cloud(self):
        self.upload_to_AWS()
        self.upload_to_Google()
        self.upload_to_Azure()

    def upload_to_AWS(self):
        r_name = os.getenv("AWS_REGION")
        bucket_name = os.getenv("AWS_BUCKET")
        
        s3 = boto3.client('s3', region_name=r_name)

        for filename in os.listdir(self.directory):
            local_file_path = os.path.join(self.directory,filename)
        
            s3_file_path = self.directory + "/" +filename
            try:
                # Upload  file to S3
                s3.upload_file(local_file_path, bucket_name, s3_file_path)
                print("AWS Table: "+ local_file_path+ " uploaded")
            except Exception as e:
                print("Error: " + local_file_path)


    def upload_to_Google(self):
        current_directory = os.getcwd()
        # base_dir = os.path.abspath(os.path.join(current_directory, "../../sensitive_data"))
        base_dir = "/home/cephuez/sensitive_data"
        key_path = base_dir + self.google_json_name

        client = storage.Client.from_service_account_json(key_path)

        directory = self.directory
        bucket = client.bucket(os.getenv("GOOGLE_BUCKET"))
        # Upload the file 
        for file_name in os.listdir(directory):
            file_path = os.path.join(directory,file_name)
        
            try:
                # Upload  file to Google Cloud
                blob = bucket.blob(file_path) # File path will also create the folders where this file will be stored
                blob.upload_from_filename(file_path)
                print("Google Table: " + file_path + " uploaded")
            except Exception as e:
                print("Error: " + file_name)        

    def upload_to_Azure(self):
        # Find string somewhere else
        connection_string = os.getenv("AZURE_CONNECTION_STRING")
        container_name = os.getenv("AZURE_CONTAINER")
        
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        directory = self.directory

        # Upload the file 
        for blob_name in os.listdir(directory):
            local_file_path = os.path.join(directory,blob_name)
            
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_path)

            try:
                # Upload  file to S3
                with open(local_file_path, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
                print("Azure Table: "+ local_file_path+ " uploaded")
            except Exception as e:
                print("Error: " + local_file_path)