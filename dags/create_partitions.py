from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task
from datetime import datetime

@dag(
    start_date=datetime(2025, 12, 1),
    schedule='0 0 25 12 *',  # Chạy lúc 00:00 ngày 25/12 hàng năm
    catchup=False,
    tags=['weather', 'maintenance'],
    dag_id='create_annual_partitions_dag'
)

def dag_partition():

    @task
    def create_partitions_for_next_year():
        # Lấy năm hiện tại và cộng thêm 1 để chuẩn bị cho năm sau
        next_year = datetime.now().year + 1

        try:
            # Khởi tạo Hook kết nối tới RDS
            postgres_hook = PostgresHook(postgres_conn_id="my_aws_rds")
            print(f"=============== TẠO PARTITION CHO NĂM {next_year} ===============")
            
            # 1. Tạo Partition THEO NĂM cho bảng fact_day
            sql_yearly = f"""
                CREATE TABLE IF NOT EXISTS gold.fact_day_{next_year} 
                PARTITION OF gold.fact_day 
                FOR VALUES FROM ('{next_year}-01-01') TO ('{next_year + 1}-01-01');
            """
            postgres_hook.run(sql_yearly)
            print("[+] Đã tạo thành công partition năm cho gold.fact_day")

            # 2. Chuẩn bị danh sách các lệnh tạo 12 Partition THEO THÁNG
            tables_monthly = [
                'bronze.raw_weather_data',
                'silver.transform_weather_data',
                'gold.fact_weather_hourly'
            ]
            
            monthly_sqls = [] # Chứa tất cả các câu lệnh SQL để chạy 1 lần
            
            for month in range(1, 13):
                # Tính toán ngày đầu tháng hiện tại và ngày đầu tháng kế tiếp
                m_str = f"{month:02d}"
                next_m = month % 12 + 1
                next_y_for_month = next_year if month < 12 else next_year + 1
                next_m_str = f"{next_m:02d}"

                start_date = f"{next_year}-{m_str}-01"
                end_date = f"{next_y_for_month}-{next_m_str}-01"

                for table in tables_monthly:
                    schema, table_name = table.split('.')
                    partition_name = f"{schema}.{table_name}_{next_year}_{m_str}"

                    sql_monthly = f"""
                        CREATE TABLE IF NOT EXISTS {partition_name} 
                        PARTITION OF {table} 
                        FOR VALUES FROM ('{start_date}') TO ('{end_date}');
                    """
                    monthly_sqls.append(sql_monthly)
                    
            # Dùng Hook chạy toàn bộ list SQL trong một Transaction duy nhất
            postgres_hook.run(monthly_sqls)
            print(f"[+] Đã tạo xong {len(monthly_sqls)} partitions hàng tháng cho nhóm bảng dữ liệu thô/giờ.")

        except Exception as e:
            print(f"[-] Lỗi trong quá trình tạo partition: {e}")
            raise ValueError(e)
    
    create_partitions_for_next_year()

dag_partition()