 
import os 
from dotenv import load_dotenv 
import requests
import json 
import boto3
import datetime
import pandas as pd
import psycopg2 
import psycopg2.extras as extras
import numpy as np

import datetime

from concurrent.futures import ThreadPoolExecutor , as_completed


def extract_data_from_api(s3_client, bucket_name, api_key , city):
    try:


        if not api_key:
            raise ValueError('Not found API key !!!')
        
        # 2. Lắp ráp đường link bằng f-string
        URL = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"

        response = requests.get(URL)
        
        if int(response.status_code) != 200:
            raise ValueError("Crawl data failure !!!!")
        data = json.loads((response.text))

        
        dt = data['dt']
        now = datetime.datetime.now()
        year = now.year
        month =now.month
        day = now.day
        file_name_on_s3 = f'{city}/{year}/{month}/{day}/{dt}.json'
        s3_client.put_object(
        Bucket=bucket_name,
        Key=file_name_on_s3,
        Body= json.dumps(data)
        )

        return {
            'filename': file_name_on_s3
        }
    except Exception as e :
        print(e)
        return False
    



def pull_from_s3(s3_client , bucket_name, file_name):
    try :
        response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        file_content = response['Body'].read()
        raw = json.loads(file_content)

        return raw 
    except Exception as ex:
        print('[Error] : ' + ex)
        return False

def extract_data_from_api_push_and_pull_s3(s3_client , bucket_name, api_key , city):
    file_name = extract_data_from_api(s3_client, bucket_name, api_key , city)
    if file_name:
        return pull_from_s3(s3_client=s3_client, bucket_name=bucket_name ,file_name= file_name['filename'])
    else:
        return False
def load_raw_data_db(s3_client , bucket_name, api_key,cities  ):
    try:
        print(f'{datetime.datetime.now()}  : Start crawl data from API and push data to S3 ')
        with ThreadPoolExecutor(max_workers= 100) as excutor :
            futures = [
                excutor.submit(
                    extract_data_from_api_push_and_pull_s3,
                    s3_client,
                    bucket_name,
                    api_key,
                    city
                )
                for  city in cities
            ]
        success = 0
        for future in futures:
            success +=1 if future.result() else success

        print(f'{datetime.datetime.now()}  : Pull success from s3 : {success} items')
        print(f'{datetime.datetime.now()}  : Transform data to dataframe')
        data = {
'longitude' :[],
'latitude':[],
'city' :[],
'country' :[],
'data_timestamp':[], 
'timezone' :[],
'temperature':[],
'pressure':[],
'humidity':[],
'sea_level':[],
'grnd_level':[],
'wind_speed':[],
'wind_deg':[],
'wind_gust':[],
'clouds':[],
'weather_condition':[],
'description':[],
'sunrise' :[],
'sunset' :[]
        }
        for future in as_completed(futures):
            raw = future.result()
            # print(raw)
            data['longitude'].append(raw['coord']['lon'] if raw.get('coord').get('lon') else None  )
            data['latitude'].append(raw['coord']['lat'] if raw.get('coord').get('lat') else None  )
            data['city'].append(raw['name'] if raw.get('name') else None  )
            data['country'].append(raw['sys']['country'] if raw.get('sys').get('country') else None  )
            data['data_timestamp'].append(raw['dt'] if raw.get('dt') else None  )
            data['timezone'].append(raw['timezone'] if raw.get('timezone') else None  )
            data['temperature'].append(raw['main']['temp'] if raw.get('main').get('temp') else None  )
            data['pressure'].append(raw['main']['pressure'] if raw.get('main').get('pressure') else None  )
            data['humidity'].append(raw['main']['humidity'] if raw.get('main').get('humidity') else None  )
            data['sea_level'].append(raw['main']['sea_level'] if raw.get('main').get('sea_level') else None  )
            data['grnd_level'].append(raw['main']['grnd_level'] if raw.get('main').get('grnd_level') else None  )
            data['wind_speed'].append(raw['wind']['speed'] if raw.get('wind').get('speed') else None  )
            data['wind_deg'].append(raw['wind']['deg'] if  raw.get('wind').get('deg') else None  )
            data['wind_gust'].append(raw['wind']['gust'] if raw.get('wind').get('gust') else None  )
            data['clouds'].append(raw['clouds']['all'] if  raw.get('clouds').get('all') else None  )
            data['weather_condition'].append(raw['weather'][0]['main'] if raw.get('weather')[0].get('main') else None  )
            data['description'].append(raw['weather'][0]['description'] if  raw.get('weather')[0].get('description') else None  )
            data['sunrise'].append(raw['sys']['sunrise'] if  raw.get('sys').get('sunrise') else None  )
            data['sunset'].append(raw['sys']['sunset'] if raw.get('sys').get('sunset') else None  )

        df = pd.DataFrame(data)

        df = df.replace({np.nan: None})

        print(f'{datetime.datetime.now()}  : Transform to data success')
        return df 
    except Exception as e:
        print('[Error]  : ' + e)
        return False

def load_to_rds(df , db_username , db_password , db_endpoint, db_name ,db_port):
    try:
        if not db_username:
            raise 'db_username is not exists, pls try again'
        if not db_password:
            raise 'db_password is not exists, pls try again'
        if not db_endpoint:
            raise 'db_endpoint is not exists, pls try again'
        if not db_name:
            raise 'db_name is not exists, pls try again'
        if not db_port:
            raise 'db_port is not exists, pls try again'
        url = f'postgresql://{db_username}:{db_password}@{db_endpoint}:{db_port}/{db_name}'
        conn = psycopg2.connect(
            host = db_endpoint,
            database = db_name,
            user = db_username,
            password = db_password,
            port =db_port
        )
        conn.autocommit = True
        cursor = conn.cursor()
        with open(os.path.join(curr_dir , '../sql/create_tables.sql'), 'r') as sql_f:
            query = sql_f.readlines()
        query = ' '.join(query)
        cursor.execute(query=query)

        print('Create all tables successfull')

        print(f'{datetime.datetime.now()}  :Start push data to RDS')
        cursor.execute(query= 'select count(*) from bronze.raw_weather_data')
        row_before = int(cursor.fetchone()[0])
        cols = [
            'city', 'country', 'latitude', 'longitude', 'temperature', 'pressure',
            'sea_level', 'grnd_level', 'humidity', 'wind_speed', 'wind_deg', 'wind_gust',
            'clouds', 'weather_condition', 'description', 'sunrise', 'sunset',
            'timezone', 'data_timestamp'
        ]

        df_insert = df[cols].where(pd.notnull(df), None)


        # Câu lệnh INSERT tham số hóa
        insert_query = """
            INSERT INTO bronze.raw_weather_data (
                city, country, latitude, longitude, temperature, pressure,
                sea_level, grnd_level, humidity, wind_speed, wind_deg, wind_gust,
                clouds, weather_condition, descriptions, sunrise, sunset,
                timezone, data_timestamp
            ) VALUES %s
        """
        row_template = """(
            %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, 
            to_timestamp(%s), 
            to_timestamp(%s), 
            %s, 
            to_timestamp(%s)
        )"""


        tuples = [tuple(x) for x in df_insert.to_numpy()]


        extras.execute_values(cursor, 
                              insert_query,
                              tuples,
                              template=row_template)
        conn.commit()
        cursor.execute(query= 'select count(*) from bronze.raw_weather_data')
        row_after = int(cursor.fetchone()[0])

        print(f'{datetime.datetime.now()}  : Insert success  {row_after - row_before} rows ' )
    except Exception as e:
        print(e)
        return False
    finally:
        # 5. Đóng kết nối
        if conn is not None:
            cursor.close()
            conn.close()
            print("Successfull to close connection.")


def extract(s3_client,bucket_name,api_key, cities, db_username,db_password, db_endpoint,db_name, db_port):
    try:
        print(f'{datetime.datetime.now()}  ==========================START EXTRACT==========================')
        df = load_raw_data_db(s3_client= s3_client , bucket_name= bucket_name ,api_key= api_key , cities= cities)

        load_to_rds(df= df , 
    db_username= db_username , db_password= db_password, db_endpoint= db_endpoint , db_name= db_name 
    , db_port= db_port)
        
        print(f'{datetime.datetime.now()}  ==========================END EXTRACT==========================')

    except Exception as e:
        print(f'{datetime.datetime.now()} [Error] : {e}')



#================= RUN TEST FUNCTION =================

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

curr_dir  = os.path.dirname(os.path.abspath(__file__))
dot_env_loc = os.path.join(curr_dir , '../config/.env')
load_dotenv(dot_env_loc)
api_key =os.getenv('API_KEY')

access_key = os.getenv('AWS_ACCESS_KEY')
secret_key = os.getenv('AWS_SECRET_KEY')
s3_client = boto3.client('s3',
        aws_access_key_id=access_key,
        aws_secret_access_key= secret_key,
        region_name='us-east-1')

bucket_name = 'weather-pipeline-kiman'

db_username = os.getenv('DB_USERNAME')
db_password = os.getenv('DB_PASSWORD')
db_endpoint = os.getenv('DB_ENDPOINT')
db_name =os.getenv('DB_NAME')
db_port = os.getenv('DB_PORT')


extract(s3_client,bucket_name,api_key, cities, db_username,db_password, db_endpoint,db_name, db_port)