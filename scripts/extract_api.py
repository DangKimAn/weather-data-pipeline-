 
import os 
from dotenv import load_dotenv 
import requests
import json 
import boto3
import datetime
import pandas as pd
import psycopg2 
import psycopg2.extras as extras


def extract_data_from_api(s3_client, bucket_name):
    try:
        print('Start pull data')
        city = os.getenv('CITY')
        if not city:
            city = 'Ho Chi Minh City'

        api_key =os.getenv('API_KEY')
        if not api_key:
            raise ValueError('Not found API key !!!')
        
        # 2. Lắp ráp đường link bằng f-string
        URL = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"

        response = requests.get(URL)
        
        if int(response.status_code) != 200:
            raise ValueError("Crawl data failure !!!!")
        print('pull data successfull')
        data = json.loads((response.text))

        print('start push data to S3')
        
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

        print('Push to S3 successfull')
        return {
            'filename': file_name_on_s3
        }
    except Exception as e :
        print(e)
        return False
    




def load_raw_data_db(s3_client , bucket_name, file_name):
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        file_content = response['Body'].read()
        raw = json.loads(file_content)
        data = {
        'longitude' : [raw['coord']['lon']],
        'latitude': [raw['coord']['lat']],
        'city' : [raw['name']],
        'country' : [raw['sys']['country']],
        'data_timestamp' : [raw['dt']],
        'timezone' : [raw['timezone']],
        'temperature': [raw['main']['temp']],
        'pressure':[raw['main']['pressure']],
        'humidity': [raw['main']['humidity']],
        'sea_level': [raw['main']['sea_level']],
        'grnd_level': [raw['main']['grnd_level']],
        'wind_speed':[raw['wind']['speed']],
        'wind_deg':[raw['wind']['deg']],
        'wind_gust':[raw['wind']['gust']],
        'clouds': [raw['clouds']['all']],
        'weather_condition': [raw['weather'][0]['main']],
        'description': [raw['weather'][0]['description']],
        'sunrise' : [raw['sys']['sunrise']],
        'sunset' : [raw['sys']['sunset']],

        }

        df = pd.DataFrame(data)
        return df
    except Exception as e:
        print(e)
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

        print('Push data to rds')

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
        print("Insert data successfull !!!!")
    except Exception as e:
        print(e)
        return False
    finally:
        # 5. Đóng kết nối
        if conn is not None:
            cursor.close()
            conn.close()
            print("Successfull to close connection.")

#================= RUN TEST FUNCTION =================

# curr_dir  = os.path.dirname(os.path.abspath(__file__))
# dot_env_loc = os.path.join(curr_dir , '../config/.env')
# load_dotenv(dot_env_loc)
# access_key = os.getenv('AWS_ACCESS_KEY')
# secret_key = os.getenv('AWS_SECRET_KEY')
# s3_client = boto3.client('s3',
#         aws_access_key_id=access_key,
#         aws_secret_access_key= secret_key,
#         region_name='us-east-1')

# bucket_name = 'weather-pipeline-kiman'

# db_username = os.getenv('DB_USERNAME')
# db_password = os.getenv('DB_PASSWORD')
# db_endpoint = os.getenv('DB_ENDPOINT')
# db_name =os.getenv('DB_NAME')
# db_port = os.getenv('DB_PORT')
    
# file_name_on_s3 = extract_data_from_api(s3_client,bucket_name)


# if file_name_on_s3:
#     df = load_raw_data_db(s3_client= s3_client , bucket_name= bucket_name , file_name=file_name_on_s3['filename'])


# load_to_rds(df,db_username , db_password , db_endpoint, db_name ,db_port)