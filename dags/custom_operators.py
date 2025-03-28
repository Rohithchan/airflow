from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class PostgreSQLCountRows(PostgresOperator):
    def __init__(self, task_id: str, postgres_conn_id: str, sql: str, *args, **kwargs) -> None:
        super().__init__(task_id=task_id, postgres_conn_id=postgres_conn_id,sql = sql,autocommit=True,*args,**kwargs)
        self.task_id = task_id
        self.postgres_conn_id = postgres_conn_id
        self.sql = sql

    def execute(self, context):
        # Get connection from Airflow connections
        postgres_hook = PostgresHook(postgres_conn_id = self.postgres_conn_id)  
        # Define the query
        sql = self.sql  
        # Execute the query and get the count
        rows = postgres_hook.get_first(sql)  
        # Push the result to XCom
        context['ti'].xcom_push(key='num_rows', value=rows)
        return rows

