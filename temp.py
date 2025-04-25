from datetime import datetime
import os
import pandas as pd
import requests
def get_wheather_csv():
    api_key = os.environ.get('open_weather_api')
    
    city = "Bengaluru"
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"

    response = requests.get(url)
    data = response.json()

    weather_data = {
        "city": city,
        "temperature": data["main"]["temp"],
        "humidity": data["main"]["humidity"],
        "pressure": data["main"]["pressure"],
        "weather": data["weather"][0]["main"],
        "timestamp": datetime.now()
    }

    df = pd.DataFrame([weather_data])
    df.to_csv("weather_data.csv", index=False)

get_wheather_csv()    