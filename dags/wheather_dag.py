# task - historical weather data from api to csv
# pandas,code optimization
# covid-19 api fetching and transformation using pandas
# dynamically fetching data from employee data csv, performing validation cleanup and importing to db

# task - current weather data from api to csv

from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import pandas as pd
import os
import requests

from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import pandas as pd
import os
import requests

def get_wheather_csv():
    api_key = os.environ.get('open_weather_api')
    print(os.environ.get('open_weather_api'))

    city = "bengaluru"
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
    df.to_csv("/opt/airflow/dags/weather_data.csv", index=False)
   

with DAG(
    dag_id="wheather_dag.py",
    start_date=datetime(2024,1,1)


) as dag:
    get_data = PythonOperator(
        task_id="get_data",
        python_callable = get_wheather_csv

    )