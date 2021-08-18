from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.models import DAG
from datetime import datetime, timedelta
import sys
sys.path.append('/home/shifty/data_etl/Airflow_ETL/AutoMailing/sub_functions')
from automailing_fundamental_functions import constitute_mail_body, send_mail_via_exchangelib, send_starting_slack_message, send_ending_slack_message, refresh_tableau_view_page, retrieve_snapshot_from_tableau

# set up a dag id for this dag => format:AutoMailing-{{project id of AutoMailing project}} 
dag_id = 'AutoMailing-ifp_deviceinfo_testing'

project_id = dag_id.split('-')[-1]

path_of_mail_body = '/home/shifty/data_etl/Airflow_ETL/AutoMailing/html_bodies/automailing_raw.html'

# argument used in DAG can be put together in default_args exclude dag_id
default_args = {
                'description': 'first airflow docker job tester',
                'start_date': datetime(2021,8,1),
                'schedule_interval' : '30 9 * * *',
                'retries': 2,
                'retry_delay': timedelta(seconds = 20)
                }


with DAG(dag_id = dag_id, default_args = default_args) as dag:
    
    # send message to assigned slack channel for notifying of the starting of this DAG
    send_starting_message_via_slack = PythonOperator(
        task_id = 'send_slack_starting_message',
        python_callable = send_starting_slack_message,
        op_kwargs={'project_id': project_id,
                   'slack_channel': 'log-test', 
                   'slack_proxy': 'CORP'},
        provide_context = True
    )
    
    # retrieve automailing information from GoogleSheets via Docker
    acquire_automailing_information = DockerOperator(
        task_id = 'acquire_automailing_info_via_docker',
        image ='benqdatateam/automailing_handler:version_1',
        entrypoint = 'python',
        xcom_all = True, 
        command = "automailing_info_retriever.py --slack_proxy=CORP --gspread_proxy=CORP --project_id={}".format(project_id)
    )
    
    # use selenium to refresh assigned Tableau view page for further downloading
    refresh_view = PythonOperator(
        task_id = 'refresh_tableau_view_page',
        python_callable = refresh_tableau_view_page,
        op_kwargs={'slack_proxy': 'CORP'},
        provide_context = True
    )
    
    # use library: tableauserverclient to download assigned Tableau view page as png to local environment
    retrieve_snapshot = PythonOperator(
        task_id = 'retrieve_snapshot_from_tableau_server',
        python_callable = retrieve_snapshot_from_tableau,
        op_kwargs={'project_id': project_id,
                   'slack_proxy': 'CORP'},
        provide_context = True   
    )
    
    # constitute customized mail content
    constitute_mail_content = PythonOperator(
        task_id = 'constitute_mail_body',
        python_callable = constitute_mail_body,
        op_kwargs={'mail_body_path': path_of_mail_body},
        provide_context = True
    )
    
    # send exchange mail via library: exchangelib
    send_mail = PythonOperator(
        task_id = 'send_exchange_mail',
        python_callable = send_mail_via_exchangelib,
        op_kwargs={'slack_proxy': 'CORP'},
        provide_context = True   
    )
    
    # record timestamp at GoogleSheets when this DAG succeeds to finish
    update_automailing_state = DockerOperator(
        task_id = 'update_automailing_sync_state',
        image ='benqdatateam/automailing_handler:version_1',
        entrypoint = 'python',
        xcom_all = False, 
        command = "automailing_state_sync.py --slack_proxy=CORP --gspread_proxy=CORP --project_id={}".format(project_id)
    )
    
    # send message to assigned slack channel for notifying of the ending of this DAG
    send_ending_message_via_slack = PythonOperator(
        task_id = 'send_slack_ending_message',
        python_callable = send_ending_slack_message,
        op_kwargs={'project_id': project_id,
                   'slack_channel': 'log-test', 
                   'slack_proxy': 'CORP'},
        provide_context = True
    )
    
    # clear all relative data of this DAG stored at XCOM
    clear_xcom_data = SqliteOperator(task_id = 'clear_xcom_data', 
                              sqlite_conn_id = 'airflow_sqlite', 
                              sql = ''' DELETE FROM XCOM WHERE dag_id = '{}' '''.format(dag_id)
    )
    
    # mark as succeed even fails on clearing relative data of this DAG at XCOM 
    successful_finish_etl_job_task = DummyOperator(task_id = "succesful_etl_job", trigger_rule = 'all_done')

    
send_starting_message_via_slack >> acquire_automailing_information >> refresh_view >> retrieve_snapshot
retrieve_snapshot >> constitute_mail_content >> send_mail >> update_automailing_state >> send_ending_message_via_slack
send_ending_message_via_slack >> clear_xcom_data >> successful_finish_etl_job_task