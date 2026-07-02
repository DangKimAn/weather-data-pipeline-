 
import os 
from dotenv import load_dotenv 
import requests
import json 
import boto3
import datetime
import pandas
dot_env_loc = '../config/.env'

load_dotenv(dot_env_loc)
access_key = os.getenv('AWS_ACCESS_KEY')
secret_key = os.getenv('AWS_SECRET_KEY')
s3_client = boto3.client('s3',
        aws_access_key_id=access_key,
        aws_secret_access_key= secret_key,
        region_name='us-east-1')

bucket_name = 'weather-pipeline-kiman'

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
    


file_name_on_s3 = extract_data_from_api(s3_client,bucket_name)

print(file_name_on_s3)

