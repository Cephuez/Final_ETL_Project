import os

class Setup_Weather_Data:
    df = ""
    weather_result_filename = ""
    def __init__(self, spark, filename, timestamp):
        self.create_data_frame(spark, filename)
        self.create_result_directory(timestamp)

    def create_data_frame(self, spark, filename):
        try:
            self.df = spark.read.json(filename)
        except AnalysisException  as e:
            # Path does not exists
            print(e)

    def create_result_directory(self, timestamp):
        self.weather_result_filename = os.path.join('weather_results', f"50_City_Results_{timestamp}")
        if not os.path.exists(self.weather_result_filename):
            os.makedirs(self.weather_result_filename)

    def get_data_frame(self):
        return self.df
    
    def get_weather_result_filename(self):
        return self.weather_result_filename

    