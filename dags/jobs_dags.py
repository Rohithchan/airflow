import uuid
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.bash import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import XCom
from custom_operators import PostgreSQLCountRows  # importing customer operator from this project

configs = {
    'dag_id_1': {'schedule_interval': "@daily", "start_date": datetime(2018, 11, 11),"table_name": "table_name_1"},  
    'dag_id_2': {'schedule_interval': "@daily", "start_date": datetime(2018, 11, 11),"table_name": "table_name_2"}, 
    'dag_id_3': {'schedule_interval': "@daily", "start_date": datetime(2018, 11, 11),"table_name": "table_name_3"}}


# Define function to insert to Postgres

def insert_to_postgres(ti):
    # Generate a unique custom_id
    custom_id = uuid.uuid4().int % 123456789
    username = ti.xcom_pull(key = 'return_value',task_ids='get_current_user')
    # datetime = datetime.now()

    sql = f"INSERT INTO postgresoperator_test(custom_id,user_name,timestamp) VALUES ('{custom_id}','{username}','{datetime.now()}')" 
    PostgresOperator (
        task_id ='insert_to_postgres',
        postgres_conn_id ='airflow_postgres',
        sql = sql
        # params=(custom_id, username, datetime.now())  # Pass parameters as a tuple
    ).execute(context={'ti': ti})


def check_table_exists(postgres_conn_id, table_name):
    hook = PostgresHook(postgres_conn_id = 'airflow_postgres')
    sql = f"""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = '{table_name}'
        );
    """
    result = hook.get_first(sql=sql)    
    if bool(result[0]) == True: # Extract the boolean value from the result tuple
        return 'insert_to_postgres_task'
    else:
        return 'create_table'



def get_num_rows(**kwargs):
    # Get connection from Airflow connections
    postgres_hook = PostgresHook(postgres_conn_id = 'airflow_postgres')  
    # Define the query
    sql = "SELECT COUNT(*) FROM postgresoperator_test;"   
    # Execute the query and get the count
    rows = postgres_hook.get_first(sql)[0]  
    # Push the result to XCom
    kwargs['ti'].xcom_push(key='num_rows', value=rows)



for config_name, config in configs.items():
    dag_id = f"{config_name}"
    interval = config["schedule_interval"]
    Start_Date = config["start_date"]
    database_name  = config["table_name"]

    # def print_process_start():
    #     print(f"{dag_id} start processing tables in database: {database_name}")


    with DAG(
        dag_id = dag_id,
        start_date =Start_Date,
        default_args={"queue":"jobs_queue"}, # Set queue for all tasks
        schedule_interval = None
    ) as dynamicDAG:

        @dynamicDAG.task
        # print_process_start = PythonOperator(
        #     task_id = "print_process_start",
        #     python_callable = print_process_start   
        # )
        def print_process_start():
            print(f"{dag_id} start processing tables in database: {database_name}")
        
        print_process_start = print_process_start()

        # Task to run the whoami command
        get_username_task = BashOperator(
                task_id='get_current_user',
                bash_command='whoami',  # This command gets the username of the airflow executor
        )


        # Checking the table if exists or not in postgres database
        check_table_exists_task = BranchPythonOperator(
            task_id="check_table_exists",
            python_callable = check_table_exists,
            op_args = ["airflow_postgres", "postgresoperator_test"],  # Replace with your connection ID and table name
        )
 

        # creating a table in postgressql database 

        create_table = PostgresOperator(
            task_id='create_table',
            postgres_conn_id = 'airflow_postgres',
            sql = '''CREATE TABLE IF NOT EXISTS postgresoperator_test(
                custom_id integer NOT NULL,
                user_name VARCHAR (50) NOT NULL,
                timestamp TIMESTAMP NOT NULL);''',
        )

        # Insert username to Postgres with PostgresOperator

        insert_to_postgres_task = PythonOperator(
            task_id ='insert_to_postgres_task',
            python_callable = insert_to_postgres,
            trigger_rule = TriggerRule.ALL_DONE
        )
 

        # Querying the postgres database table data by using python operator

        # query_table = PythonOperator(
        #     task_id ='query_table',
        #     python_callable = get_num_rows,
        #     provide_context = True
        # )


        # Querying the postgres database table data by using Custom operator (PostgreSQLCountRows)

        query_table = PostgreSQLCountRows(
            task_id = 'query_table',
            postgres_conn_id = 'airflow_postgres',
            sql = "SELECT COUNT(1) FROM postgresoperator_test"
        )


        # Set task dependencies

        print_process_start >> get_username_task >> check_table_exists_task
        check_table_exists_task >> create_table >> insert_to_postgres_task 
        check_table_exists_task >> insert_to_postgres_task >> query_table
        

