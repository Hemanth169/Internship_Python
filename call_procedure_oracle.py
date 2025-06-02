from util import oracleprocedure,connect_oracle
from Log.logscript import write_log
import pandas as pd

def main():
    write_log('-------Oracle Procedure Calling-----------')
    results = oracleprocedure()

    if results:
        write_log("Results are fetched check the output\n")
        for row in results:
            write_log(f"Department ID: {row['dept_id']} - Status: {row['STATUS']}")
        write_log('\n')
    else:
        write_log("No results returned or an error occurred.")
        return 
    
    # Filter active departments
    #active_depts = [row['dept_id'] for row in results if row['STATUS'] == 'ACTIVE']
    active_depts = [row['dept_id'] for row in results if row['STATUS'] == 'ACTIVE']

    if not active_depts:
        write_log("No active departments found. Exiting.")
        return
    
    # Connect to Oracle DB
    engine_oracle=connect_oracle()
    if engine_oracle:
        # write log for successful connection
        write_log("Connected to Oracle database.")
        write_log(f"Active departments: {active_depts}")
        
        # Construct dynamic query for active departments
        dept_ids= ','.join([str(d) for d in active_depts])
        # define query to get Employee data
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
                        e.E_ID """
   
        try:
            # Fetch data using the utility function
            import util
            data = util.fetch_data(engine_oracle, query)

            if not data.empty:
                # Export to CSV
                data.to_csv("active_dept_data.csv", index=False)
                write_log("Employee data exported successfully to 'active_dept_data.csv'.")
            else:
                write_log("No employee data found for active departments.")

        except Exception as e:
            write_log(f"Error during data fetch: {e}")
    else:
        write_log("Failed to connect to Oracle database.")


if __name__ == "__main__":
    main()

    
