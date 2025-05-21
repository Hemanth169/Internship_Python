from util import connect_oracle, fetch_data, transform_data, save_data_oracle,save_data_mysql,connect_mysql
from Log.logscript import write_log

write_log("------Data Transformation------")

def run_transformation(engine, db_name, save_func):
    try:
        write_log(f"{db_name} data transformation started")
        query = "SELECT * FROM employee"
        df = fetch_data(engine, query)
    
        column_map = {
            "e_NAME": "Employee_name",
            "e_ID": "Employee_id",
            #"E_salary": "Salary",
            "e_DATE": "Joining_Date"
        }
        date_cols = ['joining_date']
        df.columns = df.columns.str.lower()
        df_transformed = transform_data(df, column_map, date_cols)

        if df_transformed.isnull().any().any():
            write_log(f" Null values found in {db_name} data")
        #if (df_transformed['salary'] < 0).any():
            #write_log(f"Negative salary found in {db_name} data")

        save_func(df_transformed, "Employee_table_transformed", engine)
        write_log(f" {db_name} transformation completed \n")

    except Exception as e:
        write_log(f"Error in {db_name} transformation: {str(e)}\n")


if __name__ == "__main__":
    engine_oracle=connect_oracle()
    if engine_oracle:
        run_transformation(engine_oracle, "Oracle", save_data_oracle)
    engine_mysql=connect_mysql()
    if engine_mysql:
        run_transformation(engine_mysql, "MySQL", save_data_mysql)
