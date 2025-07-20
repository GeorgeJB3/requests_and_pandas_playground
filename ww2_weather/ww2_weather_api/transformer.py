from weather_api_client import api_call
import pandas as pd

all_data = api_call()

daily = all_data.Daily()

hourly = all_data.Hourly()


print(daily.Variables(0).ValuesAsNumpy())
print(hourly.Variables(0).ValuesAsNumpy())
