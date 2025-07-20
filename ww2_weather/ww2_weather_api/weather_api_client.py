import openmeteo_requests

openmeteo = openmeteo_requests.Client()

url = "https://archive-api.open-meteo.com/v1/archive"
params = {
	"latitude": 51.5085,
	"longitude": -0.1257,
	"start_date": "1940-01-01",
	"end_date": "1945-09-02",
	"daily": ["weather_code", "temperature_2m_mean", "temperature_2m_max", "temperature_2m_min", "sunrise", "sunset", "daylight_duration", "wind_speed_10m_max", "wind_direction_10m_dominant"],
	"hourly": ["temperature_2m", "rain", "snowfall", "weather_code", "wind_speed_10m", "wind_direction_10m", "relative_humidity_2m", "snow_depth"],
	"timezone": "Europe/London"
}

def api_call():
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]
    return response