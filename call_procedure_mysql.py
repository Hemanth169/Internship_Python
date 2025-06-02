from util import mysqlprocedure,connect_mysql
from Log.logscript import write_log
import pandas as pd


def main():
    write_log('-------SQL Procedure Calling-----------')
    results = mysqlprocedure()

    if results:
        write_log("Results are fetched check the output\n")
        for row in results:
            write_log(f"Department ID: {row['dept_id']} - Status: {row['STATUS']}")
        write_log('\n')
    else:
        write_log("No results returned or an error occurred.")
        return

    # Filter active departments
    active_depts = [row['dept_id'] for row in results if row['STATUS'] == 'ACTIVE']

    if not active_depts:
        write_log("No active departments found. Exiting.")
        return

    engine_mysql = connect_mysql()
    if engine_mysql:
        write_log("Connected to MySQL database.")
        write_log(f"Active departments: {active_depts}")

        dept_ids = ','.join([str(d) for d in active_depts])
        query = f"""SELECT 
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
                        e.E_ID DESC"""

        try:
            import util
            data = util.fetch_data(engine_mysql, query)

            if not data.empty:
                data.to_csv("active_dept_data_mysql.csv", index=False)
                write_log("Employee data exported successfully to 'active_dept_data_mysql.csv'.")
            else:
                write_log("No employee data found for active departments.")

        except Exception as e:
            write_log(f"Error during data fetch: {e}")
    else:
        write_log("Failed to connect to MySQL database.")


if __name__ == "__main__":
    main()
