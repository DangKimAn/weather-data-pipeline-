from airflow.decorators import dag, task
from pendulum import datetime

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from include.scripts.extract_api import extract
from include.scripts.transform import transform
from include.scripts.load import load

cities = [
                "Abu Dhabi", "Accra", "Amsterdam", "Antwerp", "Athens", "Atlanta", 
                "Auckland", "Bangkok", "Barcelona", "Beijing", "Bengaluru", "Berlin", 
                "Bogota", "Boston", "Brisbane", "Bruges", "Brussels", "Budapest", 
                "Buenos Aires", "Cairo", "Calgary", "Cape Town", "Casablanca", 
                "Chicago", "Christchurch", "Copenhagen", "Dallas", "Delhi", "Denver", 
                "Doha", "Dubai", "Dublin", "Edinburgh", "Florence", "Frankfurt", 
                "Geneva", "Hanoi", "Havana", "Helsinki", "Ho Chi Minh City", 
                "Hong Kong", "Honolulu", "Houston", "Istanbul", "Jakarta", "Jerusalem", 
                "Johannesburg", "Kuala Lumpur", "Kyoto", "Lagos", "Las Vegas", "Lima", 
                "Lisbon", "London", "Los Angeles", "Macau", "Madrid", "Manchester", 
                "Manila", "Marrakech", "Melbourne", "Mexico City", "Miami", "Milan", 
                "Montreal", "Moscow", "Mumbai", "Munich", "Nairobi", "New York", 
                "Osaka", "Oslo", "Paris", "Perth", "Prague", "Reykjavik", 
                "Rio de Janeiro", "Riyadh", "Rome", "San Francisco", "San Juan", 
                "Santiago", "Sao Paulo", "Seattle", "Seoul", "Shanghai", "Singapore", 
                "Stockholm", "Sydney", "Taipei", "Tel Aviv", "Tokyo", "Toronto", 
                "Vancouver", "Venice", "Vienna", "Warsaw", "Washington", "Wellington", 
                "Zurich"
            ]
@dag(

    start_date=datetime(2026, 1, 1),
    schedule='@hourly',
    catchup=False,
    tags=['weather', 'aws'],
    dag_id ='weather_dag'
)

def weather_dag():
    @task
    def task_extract(cities):
        try:
            bucket_name = Variable.get('BUCKET_NAME')
            
            print(bucket_name)
            api_key = Variable.get('API_KEY')
            print(api_key)
            s3_hook = S3Hook(aws_conn_id = 'my_aws_s3')
            postgres_hook = PostgresHook(postgres_conn_id="my_aws_rds")
            

            print(s3_hook)
            print(cities)
            print(postgres_hook)
            extract(s3_client=s3_hook , bucket_name= bucket_name , api_key= api_key, cities=cities , conn=postgres_hook)
            return True
        except Exception as e:
            raise ValueError(e)
    @task

    def transform_data():
        try:
            postgres_hook = PostgresHook(postgres_conn_id="my_aws_rds")
            transform(postgres_hook)
        except Exception as e:
            raise ValueError(e)

    @task

    def load_data():
        try:
            postgres_hook = PostgresHook(postgres_conn_id="my_aws_rds")
            load(postgres_hook)
        except Exception as e:
            raise ValueError(e)
    extract_task = task_extract(cities)
    transform_task = transform_data()
    load_task = load_data()
    extract_task >> transform_task >> load_task
    # load_task = load_data()

    # load_task 

weather_dag()