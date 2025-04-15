
import logging
from airflow import DAG
from datetime import datetime, timedelta,timezone
from airflow.utils.dates import days_ago
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.subdag import SubDagOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor
from slack import WebClient
from slack.errors import SlackApiError
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup
from smartsensor import SmartFileSensor # importing smartsensor custom operator


downstream_dag_id = "dag_id_1"
FileName = Variable.get('SourceFilePath', default_var='run')
RemoveFileName = '/opt/airflow/config/' + FileName
TRIGGERED_DAG_ID = "trigger_dag"
SLACK_CONNECTION_ID = "Slack_Connection"


def print_context(ti,**context):
    print(f"Triggered DAG run_id: {ti.xcom_pull(task_ids='Trigger_DAG',key = 'trigger_run_id')}")
    """Prints the full context dictionary of the task."""
    print("Full context:")
    for key, value in context.items():
        print(f"\t{key}: {value}")


def get_execution_date(current_execution_date) :
    get_date = current_execution_date.strftime("%Y-%m-%d ")
    return datetime.strptime(get_date, "%Y-%m-%d ").replace(tzinfo=timezone.utc)


# Function for sending an Notification to Slack
def send_slack_notification(dag_run, **kwargs):
    # Retrieve DAG ID and execution date from context
    dag_id = dag_run.dag_id
    execution_date = dag_run.execution_date.isoformat()
 
    # Establish connection using Airflow's connection store

    # slack_connection = BaseHook.get_connection(SLACK_CONNECTION_ID)
    # slack_token = slack_connection.extra_dejson["token"]  # Access token securely

    # Establish a connection using airflow variables in dags from Vault secret backend

    slack_token = Variable.get("slack_token")
 
    # Create Slack client with token
    client = WebClient(token=slack_token)

    # Constructinh message
    message = f"DAG '{dag_id}' completed successfully! Execution date: {execution_date}"
    # Send message to Slack channel (replace with your desired channel)
    try:
        response = client.chat_postMessage(channel="airflow_monitoring", text=message)
        logging.info(f"Successfully sent Slack notification: {response}")
    except SlackApiError as e:
        logging.error(f"Failed to send Slack notification: {e}")


# def sub_dag_factory(parent_dag_name, child_dag_name, start_date):
#     with DAG(
#         dag_id = f"{parent_dag_name}.{child_dag_name}",
#         start_date=start_date,
#         schedule_interval=None,
#         # parent_dag_id = parent_dag_name,
#     ) as child_dag:
 
#         # Sensor to validate triggered DAG success
#         validate_status = ExternalTaskSensor(
#             task_id="validate_status",
#             external_dag_id = downstream_dag_id,
#             external_task_id = None,
#             # execution_delta = timedelta(seconds=0),  # Adjust as needed
#             execution_date_fn = get_execution_date,
#             timeout = 60  # Adjust timeout as needed
#         )
 
#         # Print result from triggered DAG using XCom
#         print_result = PythonOperator(
#             task_id ="print_result",
#             python_callable = print_context
#         )
 
#          # Remove the trigger file after use 
#         remove_trigger_file = BashOperator(
#             task_id = "Remove_Run_file",
#             bash_command = f"rm {RemoveFileName}"  # Replace with your desired path
#         )

#         # Create 'finished_<timestamp>' file
#         create_finished_file = BashOperator(
#             task_id="create_finished_file",
#             bash_command = "touch finished_{{ts_nodash}}"
#         )
 
#         # Define task dependencies
#         validate_status >> print_result
#         print_result >> [remove_trigger_file, create_finished_file]
 
#     return child_dag
 
# By using Task group

def process_results_tasks():
        # Sensor to validate triggered DAG success
        validate_status = ExternalTaskSensor(
            task_id="validate_status",
            external_dag_id = downstream_dag_id,
            external_task_id = None,
            # execution_delta = timedelta(seconds=0),  # Adjust as needed
            execution_date_fn = get_execution_date,
            timeout = 60  # Adjust timeout as needed
        )
 
        # Print result from triggered DAG using XCom
        print_result = PythonOperator(
            task_id ="print_result",
            python_callable = print_context,
            provide_context=True,  # Pass DAG run context to the function
        )
 
         # Remove the trigger file after use 
        remove_trigger_file = BashOperator(
            task_id = "Remove_Run_file",
            bash_command = f"rm {RemoveFileName}"  # Replace with your desired path
        )

        # Create 'finished_<timestamp>' file
        create_finished_file = BashOperator(
            task_id="create_finished_file",
            bash_command = "touch finished_{{ts_nodash}}"
        )

        # Defining task dependencies
        validate_status >> print_result
        print_result >> [remove_trigger_file, create_finished_file]



# Main DAG with SubDAG/TaskGroup definition

default_args = {
    'owner': 'rohith',
    'depends_on_past': False,
    'start_date': days_ago(1), # We set it to one day ago to allow manual triggering.
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


with DAG(
    dag_id = "trigger_dag",
    default_args=default_args,
    schedule_interval = None,  # Set to None as we don't want automatic scheduling.
) as maindag:
 
    # File sensor to wait for the trigger file

    # trigger_file_sensor = FileSensor(
    #     task_id="Sensor_Wait_Run_File",
    #     filepath = FileName ,   # getting the file path from the Variable 
    #     # filepath = '/opt/airflow/config/run'
    #     fs_conn_id = 'FlatFileConnection',
    #     poke_interval = 1
    # )

    # Sensing a file by using Smart Sensor Operator

    trigger_file_sensor = SmartFileSensor(
        task_id="Sensor_Wait_Run_File",
        filepath = FileName,  # getting the file path from the Variable 
        # filepath = '/opt/airflow/config/run'
        fs_conn_id = 'FlatFileConnection'
        # poke_interval = 1
    )
 
    # Trigger the downstream DAG

    trigger_downstream_dag = TriggerDagRunOperator(
        task_id="Trigger_DAG",
        trigger_dag_id = downstream_dag_id,
        execution_date = "{{ ds }}",
        wait_for_completion = True,
        reset_dag_run = True
    )

    # triggering the SubDAG

    # trigger_subdag = SubDagOperator(
    # # PythonOperator(
    #     task_id="Process_Results_SubDAG",
    #     # python_callable = lambda: sub_dag_factory("trigger_dag", "Process_Results_SubDAG", days_ago(1)),
    #     subdag = sub_dag_factory("trigger_dag", "Process_Results_SubDAG", days_ago(1))
    # )

    # Call process_results_tasks function to using the task group

    with TaskGroup("process_results") as process_results_taskgroup:
        process_results_tasks()


    # Adding notification task at the end (or any suitable place) once task are completed

    send_slack_notification_task = PythonOperator(
        task_id="Alert_to_slack",
        python_callable=send_slack_notification,
        provide_context=True,  # Pass DAG run context to the function
    )
 
 
    # Define the task dependencies

    trigger_file_sensor >> trigger_downstream_dag >> process_results_taskgroup >> send_slack_notification_task




