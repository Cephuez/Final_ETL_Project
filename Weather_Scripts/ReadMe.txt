I use a Jupyter file to handle most of my code "Extract_Weather_Information.ipynb". 
I convert it into a python file before running it on Airflow

The project does a few things
  1. It extracts the data from OpenWeatherMap using their API
  2. I then use their JSON file to create separate tables 
  3. Using these tables, I then get important information like cities with longest Daylight Hours
  4. Finally, I upload them to my own cloud storage: AWS S3, Google Cloud Storage, Microsoft Azure

