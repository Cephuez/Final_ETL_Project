import os
import boto3
from google.cloud import storage
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

class UploadWeatherData:
    weather_result_filename = ""
    google_json_name = "/Google_Cloud_Key.json"
    
    def __init__(self, weather_result_filename):
        self.weather_result_filename = weather_result_filename

    def upload_to_AWS(self):
        # Upload to AWS
        r_name = os.getenv("AWS_REGION")
        bucket_name = os.getenv("AWS_BUCKET")

        s3 = boto3.client('s3', region_name=r_name)

        for filename in os.listdir(self.weather_result_filename):
            local_file_path = os.path.join(self.weather_result_filename,filename)
        
            s3_file_path = self.weather_result_filename + "/" +filename
            try:
                # Upload  file to S3
                s3.upload_file(local_file_path, bucket_name, s3_file_path)
                print("AWS Result: "+ local_file_path+ " uploaded")
            except Exception as e:
                print("Error: " + local_file_path)

    def upload_to_Azure(self):
        # Find string somewhere else
        connection_string = os.getenv("AZURE_CONNECTION_STRING")
        container_name = os.getenv("AZURE_CONTAINER")
        
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        directory = self.weather_result_filename
    
        # Upload the file 
        for blob_name in os.listdir(directory):
            local_file_path = os.path.join(directory,blob_name)
            
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_path)

            try:
                # Upload  file to S3
                with open(local_file_path, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
                print("Azure Result: "+ local_file_path+ " uploaded")
            except Exception as e:
                print("Error: " + local_file_path)

    def upload_to_Google(self):
        current_directory = os.getcwd()
        # base_dir = os.path.abspath(os.path.join(current_directory, "../../sensitive_data"))
        base_dir = "/home/cephuez/sensitive_data"
        key_path = base_dir + self.google_json_name

        client = storage.Client.from_service_account_json(key_path)

        directory = self.weather_result_filename
        bucket = client.bucket(os.getenv("GOOGLE_BUCKET"))
        # Upload the file 
        for file_name in os.listdir(directory):
            file_path = os.path.join(directory,file_name)
        
            try:
                # Upload  file to Google Cloud
                blob = bucket.blob(file_path) # File path will also create the folders where this file will be stored
                blob.upload_from_filename(file_path)
                print("Google Result: " + file_path)
            except Exception as e:
                print("Error: " + file_name)








        