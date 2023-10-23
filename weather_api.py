import datetime
import requests

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

#from airflow.operators.python_operator import PythonOperator




args = {
    'owner': 'dimon',
    'start_date': datetime.datetime(2023, 10, 1),
    'provide_context': True
}

api_key = Variable.get("API_WEATHER")
start_hour = 1
horizont_hours = 48

lat = 47.939
lng = 46.681
moscow_timezone = 3
local_timezone = 4
city = 'Moscow'


def extract_data(**kwargs):
    ti = kwargs['ti']
    response = requests.get(
        f'https://api.openweathermap.org/data/2.5/weather?q={city},'
        f'ru&appid={api_key}&units=metric')

    if response.status_code == 200:

        json_data = response.json()
        print(json_data)
        ti.xcom_push(key='weather_wwo_json', value=json_data)


def transform_data(**kwargs):
    ti = kwargs['ti']
    location = {}
    json_data = ti.xcom_pull(key='weather_wwo_json',
                             task_ids=['extract_data'])[0]
    location = json_data['coord']['lon'] + json_data['coord']['lat']
    temp = json_data['main']['temp']
    print(location)
    res_df = pd.DataFrame({'location': location, 'temp': temp})
    print(res_df)
    ti.xcom_push(key='weather_wwo_df', value=res_df)



def load_data(**kwargs):
    ti = kwargs['ti']
    res_df = ti.xcom_pull(key='weather_wwo_df', task_ids=['transform_data'])[0]
    print(res_df.head())


with DAG('load_weather_wwo', description='load_weather_wwo',
         schedule='*/1 * * * *', catchup=False,
         default_args=args) as dag:  # 0 * * * *   */1 * * * *
    extract_data = PythonOperator(task_id='extract_data',
                                  python_callable=extract_data)
    transform_data = PythonOperator(task_id='transform_data',
                                    python_callable=transform_data)
    # load_data = PythonOperator(task_id='load_data', python_callable=load_data)

    extract_data >> transform_data

if __name__ == "__main__":
    dag.test()
