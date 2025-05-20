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
        user = 'hemanth'
        password = '*******'
        host = '192.168.200.91'
        port = '1521'
        service_name = '*****'
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
        password = urllib.parse.quote_plus("*********")
        
        # Build the connection URI
        engine_mysql = create_engine(
            f"mysql+pymysql://hemanth:{password}@192.168.200.91/hemanth_db"
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
