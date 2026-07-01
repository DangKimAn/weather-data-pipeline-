 
import os 
from dotenv import load_dotenv 
import requests
import json 
import boto3
import datetime
dot_env_loc = '../config/.env'


def extract_data_from_api():
    try:
        load_dotenv(dot_env_loc)
        print('Start pull data')
        city = os.getenv('CITY')
        if not city:
            city = 'Ho Chi Minh City'

        api_key =os.getenv('API_KEY')
        if not api_key:
            raise ValueError('Not found API key !!!')
        access_key = os.getenv('AWS_ACCESS_KEY')
        secret_key = os.getenv('AWS_SECRET_KEY')
        # 2. Lắp ráp đường link bằng f-string
        URL = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"

        response = requests.get(URL)
        
        if int(response.status_code) != 200:
            raise ValueError("Crawl data failure !!!!")
        print('pull data successfull')
        data = json.loads((response.text))

        print('start push data to S3')
        s3_client = boto3.client('s3',
                                 aws_access_key_id=access_key,
        aws_secret_access_key= secret_key,
        region_name='us-east-1')
        dt = data['dt']
        now = datetime.datetime.now()
        year = now.year
        month =now.month
        day = now.day
        bucket_name = 'weather-pipeline-kiman'
        file_name_on_s3 = f'{city}/{year}/{month}/{day}/{dt}.json'
        s3_client.put_object(
        Bucket=bucket_name,
        Key=file_name_on_s3,
        Body= json.dumps(data)
        )

        print('Push to S3 successfull')
    except Exception as e :
        print(e)
        return False
    


extract_data_from_api()