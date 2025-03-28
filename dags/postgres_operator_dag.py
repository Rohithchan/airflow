# import uuid
# from datetime import datetime
# from airflow import DAG
# from airflow.utils.trigger_rule import TriggerRule
# from airflow.operators.postgres_operator import PostgresOperator
# from airflow.models import XCom


# with DAG (
#     dag_id = 'PostgresOperator_dag',
#     # start_date = days_ago(1),
#     start_date= datetime(2019, 10, 7),
#     schedule_interval = None
# ) as postgresdag:

#     create_table = PostgresOperator(
#         task_id='create_table',
#         postgres_conn_id = 'airflow_postgres',
#         sql='''CREATE TABLE postgresoperator_test(
#             custom_id integer NOT NULL,s
#             user_name VARCHAR (50) NOT NULL,
#             timestamp TIMESTAMP NOT NULL);''',
#     )

#     insert_row = PostgresOperator(
#         task_id='insert_row',
#         postgres_conn_id = 'airflow_postgres',
#         sql='INSERT INTO postgresoperator_test VALUES(%s, %s, %s)',
#         trigger_rule=TriggerRule.ALL_DONE,
#         parameters = (uuid.uuid4().int % 123456789
#                     , {ti.xcom_pull(task_ids='get_current_user',key = 'return_value')} 
#                     , datetime.now())
#     )

#     create_table >> insert_row