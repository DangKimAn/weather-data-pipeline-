import psycopg2
from dotenv import load_dotenv 
import os 
import re

curr_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(curr_dir, '../config/.env'))

db_username = os.getenv('DB_USERNAME')
db_password = os.getenv('DB_PASSWORD')
db_endpoint = os.getenv('DB_ENDPOINT')
db_port = os.getenv('DB_PORT')
db_name = os.getenv('DB_NAME')

def check_table_exists(cursor, schema_name, table_name):
    """Truy vấn PostgreSQL để kiểm tra sự tồn tại của bảng."""
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = %s AND table_name = %s
        );
    """, (schema_name, table_name))
    return cursor.fetchone()[0]

def execute_and_log_tables(cursor, filepath, layer_name):
    """Hàm dùng chung để đọc file SQL, tách lệnh và kiểm tra trạng thái bảng."""
    print(f'=============== RUNNING {layer_name.upper()} LAYER ===============')
    try:
        with open(filepath, 'r') as f:
            sql_script = f.read()

        # Tách các câu lệnh bằng dấu ';'
        statements = sql_script.split(';')
        for stmt in statements:
            stmt = stmt.strip()
            if not stmt:
                continue
            
            # Tìm kiếm câu lệnh CREATE TABLE để lấy tên bảng
            match = re.search(r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_.]+)', stmt, re.IGNORECASE)
            
            if match:
                full_table_name = match.group(1)
                
                # Tách schema và table name (vd: bronze.raw_weather_data)
                if '.' in full_table_name:
                    schema, table = full_table_name.split('.')
                else:
                    schema, table = 'public', full_table_name
                
                # Kiểm tra xem bảng đã có trong database chưa
                if check_table_exists(cursor, schema, table):
                    print(f"[*] Bảng '{full_table_name}' đã tồn tại. Bỏ qua...")
                    cursor.execute(stmt) # Vẫn chạy để an toàn, Postgres tự xử lý IF NOT EXISTS
                else:
                    cursor.execute(stmt)
                    print(f"[+] Tạo thành công bảng '{full_table_name}'.")
            else:
                # Chạy các câu lệnh khác (CREATE SCHEMA, v.v.)
                cursor.execute(stmt)
                if "CREATE SCHEMA" in stmt.upper():
                    print(f"[-] Đã chạy lệnh khởi tạo Schema (nếu chưa có).")
                    
        print(f'=============== {layer_name.upper()} LAYER SUCCESSFUL ===============\n')
    except Exception as e:
        print(f"Lỗi khi chạy file {filepath}: {e}")
        raise ValueError(e)

def create_table(cursor, path_dir):
    try:
        execute_and_log_tables(cursor, os.path.join(path_dir, 'bronze/create_tables.sql'), 'Bronze')
        execute_and_log_tables(cursor, os.path.join(path_dir, 'silver/create_tables.sql'), 'Silver')
        execute_and_log_tables(cursor, os.path.join(path_dir, 'gold/create_tables.sql'), 'Gold')
        print('=============== ALL TABLES CHECKED/CREATED SUCCESSFULLY ===============\n')
    except Exception as e:
        raise ValueError(e)
    

def execute_and_log_procedures(cursor, filepath, layer_name):
    """Đọc file SQL, chạy Procedures nguyên khối và in log thành công."""
    print(f'=============== RUNNING PROCEDURES {layer_name.upper()} LAYER ===============')
    try:
        if not os.path.exists(filepath):
            print(f"[-] File không tồn tại: {filepath}. Bỏ qua...")
            return

        with open(filepath, 'r') as f:
            sql_script = f.read()

        # Dùng Regex để quét tìm tất cả tên Procedure có trong file
        # Nhận diện chuỗi: CREATE OR REPLACE PROCEDURE schema_name.procedure_name
        pattern = r'CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\s+([a-zA-Z0-9_.]+)'
        procedures = re.findall(pattern, sql_script, re.IGNORECASE)

        # Chạy nguyên khối SQL thay vì cắt bằng dấu ';'
        cursor.execute(sql_script)

        # In log thành công cho từng procedure tìm được
        if procedures:
            for proc in procedures:
                print(f"[+] Đã tạo/cập nhật thành công Procedure: '{proc}'")
        else:
            print("[-] Không tìm thấy Procedure nào trong file để chạy.")
            
        print(f'=============== {layer_name.upper()} PROCEDURES SUCCESSFUL ===============\n')
        
    except Exception as e:
        print(f"Lỗi khi chạy file Procedure {filepath}: {e}")
        raise ValueError(e)
def create_procedure(cursor, path_dir):
    try:
        # Đường dẫn tới file chứa procedures của bạn
        silver_path = os.path.join(path_dir, 'silver/create_producers.sql')
        gold_path = os.path.join(path_dir, 'gold/create_producers.sql')
        
        execute_and_log_procedures(cursor, silver_path, 'Silver')
        execute_and_log_procedures(cursor, gold_path, 'Gold')
        
        print('=============== ALL PROCEDURES CREATED/UPDATED SUCCESSFULLY ===============\n')
    except Exception as e:
        raise ValueError(e)
    

def setup(db_username, db_password, db_endpoint, db_name, db_port, path_dir):
    try:
        # Validate input credentials
        missing_vars = []
        if not db_username: missing_vars.append('db_username')
        if not db_password: missing_vars.append('db_password')
        if not db_endpoint: missing_vars.append('db_endpoint')
        if not db_name: missing_vars.append('db_name')
        if not db_port: missing_vars.append('db_port')
        
        if missing_vars:
            raise ValueError(f"Thiếu thông tin kết nối: {', '.join(missing_vars)}. Vui lòng kiểm tra lại file .env")
        
        print(f'Đang kết nối đến Endpoint: {db_endpoint} | DB: {db_name}')
        print('=============== CONNECT TO DBS ===============')
        
        conn = psycopg2.connect(
            host=db_endpoint,
            database=db_name,
            user=db_username,
            password=db_password,
            port=db_port
        )
        conn.autocommit = True
        cursor = conn.cursor()
        print('=============== CONNECT TO DBS SUCCESSFUL ===============\n')
        
        create_table(cursor, path_dir)
        create_procedure(cursor, path_dir)
        
    except Exception as e:
        print(f"Quá trình Setup thất bại: {e}")
        raise



if __name__ == "__main__":
    sql_path = os.path.join(curr_dir, '../sql')
    setup(db_username, db_password, db_endpoint, db_name, db_port, sql_path)