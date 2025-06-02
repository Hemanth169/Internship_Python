from util import oracleprocedure, connect_oracle, mysqlprocedure, connect_mysql,fetch_data
from Log.logscript import write_log

def process_database(db_type, procedure_func, connect_func, output_file, sort_order='ASC'):
    write_log(f'-------{db_type} Procedure Calling-----------')
    results = procedure_func()

    if results:
        write_log("Results fetched. Output:")
        for row in results:
            write_log(f"Department ID: {row['dept_id']} - Status: {row['STATUS']}")
        write_log('\n')
    else:
        write_log("No results returned or an error occurred.")
        return

    active_depts = [row['dept_id'] for row in results if row['STATUS'] == 'ACTIVE']
    if not active_depts:
        write_log("No active departments found. Exiting.")
        return

    engine = connect_func()
    if engine:
        write_log(f"Connected to {db_type} database.")
        write_log(f"Active departments: {active_depts}")

        dept_ids = ','.join([str(d) for d in active_depts])
        query = f"""
            SELECT 
                e.E_ID,
                e.E_NAME,
                e.E_SALARY,
                e.E_DATE,
                e.DEPT_ID
            FROM 
                EMPLOYEE e
            WHERE 
                e.DEPT_ID IN ({dept_ids})
            ORDER BY 
                e.E_ID {sort_order}
        """

        try:
            data = fetch_data(engine, query)
            if not data.empty:
                data.to_csv(output_file, index=False)
                write_log(f"Employee data exported successfully to '{output_file}'.")
            else:
                write_log("No employee data found for active departments.")
        except Exception as e:
            write_log(f"Error during data fetch: {e}")
    else:
        write_log(f"Failed to connect to {db_type} database.")


if __name__ == "__main__":

    # For Oracle
    process_database(
        db_type='Oracle',
        procedure_func=oracleprocedure,
        connect_func=connect_oracle,
        output_file='active_dept_data.csv',
        sort_order='ASC'
    )

    # For MySQL
    process_database(
        db_type='MySQL',
        procedure_func=mysqlprocedure,
        connect_func=connect_mysql,
        output_file='active_dept_data_mysql.csv',
        sort_order='DESC'
    )
