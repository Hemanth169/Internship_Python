import util
from Log.logscript import write_log

write_log('------------ NEW CONNECTION ------------')

# Define query
query = "SELECT * FROM Employee"

# Connect to Oracle
engine_oracle = util.connect_oracle()
if engine_oracle is not None:
    write_log("Connected to Oracle database.")
else:
    write_log("Connection to Oracle failed.")

# Fetch data from oracle.
oracle_data = util.fetch_data(engine_oracle, query)
# Count rows
oracle_count = len(oracle_data)
write_log(f"Oracle row count: {oracle_count}")

# Connect to MySQL
engine_mysql = util.connect_mysql()
if engine_mysql is not None:
    write_log("Connected to MySQL database.")
else:
    write_log("Connection to MySQL failed.")

# Fetch data from mysql.
mysql_data = util.fetch_data(engine_mysql, query)
# Count rows 
mysql_count = len(mysql_data)
write_log(f"MySQL row count: {mysql_count}")

# Compare only by row count
if oracle_count == mysql_count:
    write_log("Row counts match between Oracle and MySQL.\n")
else:
    write_log("Row counts do not match between Oracle and MySQL.\n")
