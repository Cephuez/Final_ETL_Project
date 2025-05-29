from datetime import datetime, timedelta
from dotenv import load_dotenv

import os
import requests
import json

class Import_Weather_Data:
    # Holds the ids of the cities I'm planning on getting information from
    filename = ""
    timestamp = ""
    city_ids = [
            "5128581", "2643743", "2988507", "1850147", "2147714", "2950159", "6167865", "3448433", "3530597", "1275339",
            "360630", "1816670", "524901", "292223", "3369157", "3169070", "1609350", "745044", "5368361", "1880252",
            "3435910", "1835848", "3117735", "1642911", "4887398", "3936456", "184745", "3871336", "108410", "1735161",
            "2332459", "756135", "2761369", "3067696", "3054643", "1701668", "2964574", "658225", "2800866", "2618425",
            "3143244", "2673730", "2759794", "2657896", "3413829", "264371", "112931", "98182", "1581130", "1668341",
            "2063523", "2193733", "5856195", "6173331", "293397", "2553604"]
    def __init__(self, timestamp):
        self.get_weather_data()
        self.timestamp = timestamp

    def get_weather_data(self):
        weather_api_key = os.getenv("WEATHER_API_KEY")
        weather_directory = "weather_data"
        
        os.makedirs(weather_directory, exist_ok=True)

        self.filename = os.path.join(weather_directory, f"50_City_{self.timestamp}.json")
        with open(self.filename, "w") as f:
            for city_id in self.city_ids:
                url = f"http://api.openweathermap.org/data/2.5/weather?id={city_id}&appid={weather_api_key}&units=metric"
                response = requests.get(url)
                
                #data = response.json()
                if response.status_code == 200:
                    data = response.json()
                    f.write(json.dumps(data) + "\n")
                else:
                    print(f"❌ Failed for city ID {city_id}: {response.status_code}")

    def get_filename(self):
        return self.filename
        
    def hello(self):
        print("Hello")