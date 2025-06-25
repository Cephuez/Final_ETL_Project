import os

class Export_Weather_Data:
    weather_result_filename = "" # Location folder for all results
    
    def __init__(self, weather_result_filename):
        self.weather_result_filename = weather_result_filename

    def to_parquet(self, data, parque_name):
        path = os.path.join(self.weather_result_filename, parque_name)
        data.to_parquet(path, engine='pyarrow') 

    def to_csv(self, data, parque_name):
        path = os.path.join(self.weather_result_filename, parque_name)
        data.to_csv(path, index=False)

    def to_json(self, data, parque_name):
        path = os.path.join(self.weather_result_filename, parque_name)
        data.to_json(path, orient='records', lines=True)
    