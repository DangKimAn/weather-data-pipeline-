import psycopg2 


def transform(postgres_hook):
    try:
        row_before = postgres_hook.get_first('SELECT count(*) FROM silver.TRANSFORM_weather_data')[0]
        postgres_hook.run('call silver.transform_data()',autocommit=True)
        row_after = postgres_hook.get_first('SELECT count(*) FROM silver.TRANSFORM_weather_data')[0]

        print(f'Insert to silver.transform_weather_data success : {(row_after) - (row_before) +1 } rows ')
        
    except Exception as e:
        print('Error in transform() ')
        print(f'[Error]  : {e}')