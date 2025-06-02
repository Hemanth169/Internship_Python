import pandas as pd
from sqlalchemy import create_engine
import sys

# Add path to logscript
sys.path.append('/Users/home/Desktop/oracle/Log')
from Log.logscript import write_log

#oracle database connection.
def connect_oracle():
    try:
        # Proper Oracle SQLAlchemy connection string using oracledb driver in thin mode
        user = 'user_name'
        password = '************'
        host = 'IP address'
        port = '1521'
        service_name/service_id= '************'
        oracle_url = (f'oracle+oracledb://{user}:{password}@{host}:{port}/?service_name={service_name}')
        engine_oracle = create_engine(oracle_url)
        write_log("Connected to Oracle using SQLAlchemy")
        return engine_oracle
    except Exception as e:
        write_log("Failed to connect to Oracle: " + str(e))
        return None
    
#MYSQL database connection
import urllib.parse
def connect_mysql():
    try:
        # URL-encode password in case it contains special characters like '@'
        password = urllib.parse.quote_plus("**********")
        
        # Build the connection URI
        engine_mysql = create_engine(
            f"mysql+pymysql://user_name:{password}@host_IP_address/Database_name"
        )
        
        write_log("Connected to MySQL database using SQLAlchemy")
        return engine_mysql

    except Exception as e:
        write_log(f" Failed to connect to MySQL using SQLAlchemy: {e}")
        return None


#Fecth and compare the data.
def fetch_data(engine, query):
    try:
        df = pd.read_sql(query,engine)
        write_log("Query executed: "+query)
        return df
    except Exception as e:
        write_log("Query failed: " + str(e))
        return pd.DataFrame()

# Transform the data    
def transform_data(df, column_map, date_cols):
    try:
        write_log("Starting data transformation...")

        # Rename columns
        df = df.rename(columns=column_map)
        write_log(f"Renamed columns: {column_map}")

        # Convert date columns
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
                write_log(f"Formatted date column: {col}")
        return df
    except Exception as e:
        write_log(" Data transformation failed: " + str(e))
        return pd.DataFrame()

# save the data into another table.
def save_data_oracle(df, table_name, engine_oracle):
    try:
        df.to_sql(name=table_name, con=engine_oracle, index=False, if_exists='replace',dtype=None)
        write_log(f"Data saved to table: {table_name}")
    except Exception as e:
        write_log(" Failed to save data to table: " + str(e))
def save_data_mysql(df, table_name, engine_mysql):
    try:
        df.to_sql(name=table_name, con=engine_mysql, index=False, if_exists='replace',dtype=None)
        write_log(f"Data saved to table: {table_name}")
    except Exception as e:
        write_log(" Failed to save data to table: " + str(e))

# calling stored procedures of oracle and mysql.
# oracle procedure call.
def oracleprocedure():
    engine_oracle = connect_oracle()
    results = []

    try:
        with engine_oracle.connect() as conn:
            with conn.connection.cursor() as cursor:
                df = pd.read_sql("SELECT dept_id FROM DEPARTMENT", conn) 
                for d_id in df['dept_id']:
                    state = cursor.var(str)

                    def run_proc():
                        cursor.callproc('GET_STATUS', [d_id, state])
                        return state.getvalue()

                    try:
                        status = retry_procedure(run_proc, retries=3, db_type="Oracle")
                        results.append({'dept_id': d_id, 'STATUS': status})
                    except Exception as e:
                        print(f"Failed for dept_id={d_id}: {e}")
                conn.connection.commit()
    except Exception as e:
        print("Error:", e)
    return results

# mysql procedure call.
def mysqlprocedure():
    engine_mysql = connect_mysql()
    results = []

    try:
        with engine_mysql.connect() as conn:
            with conn.connection.cursor() as cursor:
                cursor.execute("SELECT dept_id FROM department")
                dept_ids = [row[0] for row in cursor.fetchall()]

                for d_id in dept_ids:
                    def run_proc():
                        cursor.execute("SET @status = '';")
                        cursor.execute(f"CALL Get_status({d_id}, @status);")
                        cursor.execute("SELECT @status;")
                        return cursor.fetchone()[0]

                    try:
                        status = retry_procedure(run_proc, retries=3, db_type="MySQL")
                        results.append({'dept_id': d_id, 'STATUS': status})
                    except Exception as e:
                        print(f"Failed for dept_id={d_id}: {e}")

            conn.commit()
    except Exception as e:
        print("Error:", e)
    return results


#Retrying stored procedure call function.
def retry_procedure(call_fn, retries=3, delay=2, db_type=""):
    attempts = 0
    while attempts < min(retries, 3):
        try:
            return call_fn()
        except Exception as e:
            attempts += 1
            write_log(f"[{db_type}] Attempt {attempts} failed: {e}")
            time.sleep(delay)
    raise Exception(f"[{db_type}] Procedure failed after {retries} attempts.")




