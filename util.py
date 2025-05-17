import pandas as pd
from sqlalchemy import create_engine
import sys

# Add path to logscript
sys.path.append('/Users/home/Desktop/python_internship/Log')
from Log.logscript import write_log
import urllib.parse

# Oracle Database connectivity
def connect_oracle():
    try:
        # Proper Oracle SQLAlchemy connection string using oracledb driver in thin mode
        user = 'hemanth'
        password = 'hello123'
        host = '192.168.200.91'
        port = '1521'
        service_name = 'bsplhndl'

        # Use urllib.parse.quote to safely encode the password if needed
        encoded_password = urllib.parse.quote(password)

        oracle_url = (
            f'oracle+oracledb://{user}:{encoded_password}@{host}:{port}/?service_name={service_name}'
        )

        engine = create_engine(oracle_url)
        write_log("Connected to Oracle using SQLAlchemy")
        return engine
    except Exception as e:
        write_log("Failed to connect to Oracle: " + str(e))
        return None

# Used to fetch data from the tables.
def fetch_data(engine, query):
    try:
        df = pd.read_sql(query,engine)
        write_log("Query executed: " + query)
        return df
    except Exception as e:
        write_log("Query failed: " + str(e))
        return pd.DataFrame()

# Used to transform data. 
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

# used to save data into another table
def save_data(df, table_name, engine):
    try:
        df.to_sql(name=table_name, con=engine, index=False, if_exists='replace')
        write_log(f"✅ Data saved to table: {table_name}")
    except Exception as e:
        write_log(" Failed to save data to table: " + str(e))

# OracleProcedure function, used to connect the procedure in oracledb
def oracleprocedure():
    engine = connect_oracle()
    results = []

    try:
        with engine.connect() as connection:
            with connection.connection.cursor() as cursor:
                # Fetch department IDs
                df = pd.read_sql("SELECT dept_id FROM DEPARTMENT", connection) 
                for d_id in df['dept_id']:
                    state = cursor.var(str)
                    cursor.callproc('GET_STATUS', [d_id, state])
                    results.append({'dept_id ': d_id, 'STATUS': state.getvalue()})

                connection.connection.commit()

    except Exception as e:
        print("Error:", e)

    return results


if __name__ == "__main__":
    engine = connect_oracle()
    if engine:
        query = "SELECT * FROM Employee"
        df = fetch_data(engine, query)  
        print(df)
        write_log("Finished fetching data.")
    else:
        print("Connection failed.")
