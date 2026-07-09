import psycopg2 


def load(postgres_hook):
    try:
        # x = postgres_hook.run('call  gold.insert_data(NULL , NULL , NULL, NULL , NULL, NULL);',autocommit=True)
        # print(f'x = {x}')
        # 1. Lấy kết nối gốc (psycopg2 connection object) thay vì dùng hàm wrapper
        conn = postgres_hook.get_conn()
        
        # 2. Bật autocommit (Bắt buộc cho Procedure như ta đã bàn)
        conn.autocommit = True
        
        # 3. Dùng context manager (with) để quản lý Cursor
        with conn.cursor() as cursor:
            # Thực thi Procedure
            cursor.execute('CALL gold.insert_data(NULL, NULL, NULL, NULL, NULL, NULL);')
            
            # 4. Hứng kết quả trả về bằng fetchone()
            result = cursor.fetchone()
            
            if len(result) != 6 :
                raise 'len(result) is wrong , run success but not have notification'
            else :
                if result[0] !=0:
                    print(f'gold.dim_location inserted {result[0]} rows')
                if result[1] !=0: 
                    print(f'gold.dim_location updated {result[1]} rows')
                if result[2] !=0: 
                    print(f'gold.dim_weather_conditions inserted {result[2]} rows')
                if result[3] !=0:
                    print(f'gold.fact_day inserted {result[3]} rows')
                if result[4] !=0: 
                    print(f'gold.fact_day updated {result[4]} rows')
                if result[5] !=0: 
                    print(f'gold.fact_weather_hourly inserted {result[5]} rows')
            print('Run successfull!!!')
    except Exception as e:
        print('Error in load() ')
        print(f'[Error]  : {e}')