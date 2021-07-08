import os
import csv
import time
import airflow
import google.auth
from datetime import datetime, timedelta
from google.cloud import bigquery
from airflow.models import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta

dag_id = 'first_airflow_job'
# default for whole DAG
default_args = {
                'description': 'first airflow data pipeline job',
                'start_date': datetime(2021,6,1),
                'schedule_interval' : '30 9 * * *',
                'retries': 2,
                'retry_delay': timedelta(seconds = 20)
                }

# path for saving Bigquery result
csv_file_path = '/home/hduser/raw_data/bigquery_raw.csv'

# load Bigquery SQL script
with open('/home/hduser/airflow/task/first_airflow_job/bigquery.sql', 'r') as f:

    bigquery_script = f.read()

# load Sqlite SQL script
with open('/home/hduser/airflow/task/first_airflow_job/sqlite.sql', 'r') as f:

    sqlite_script = f.read()

# return BigQuery query result as DataFrame
def pull_raw_data_from_bq(date, **kwargs):
    credentials, project = google.auth.default(
                                            scopes = [
                                                "https://www.googleapis.com/auth/drive",
                                                "https://www.googleapis.com/auth/cloud-platform",
                                                "https://www.googleapis.com/auth/bigquery",
                                                     ]
                                            )
    
    query = bigquery_script.format(date)
    
    client = bigquery.Client(credentials = credentials, project = project)
    
    df = client.query(query).to_dataframe()
    
    kwargs['ti'].xcom_push(key = 'bq_raw_data', value = df)
    
    row_count = len(df)
    print('row_count: ',row_count)
    
    if row_count:
        return 'save_as_csv'
    else:
        return 'do_nothing'

# save DataFrame to csv file
def save_df_to_csv(csv_path, is_keep_index, **kwargs):
    
    df = kwargs['ti'].xcom_pull(key = 'bq_raw_data', task_ids = 'get_bigquery_data_as_df')
    print('data review:\n', df.head(10))
    
    df.to_csv(csv_path, index = is_keep_index)  
    
    kwargs['ti'].xcom_push(key = 'file_save_status', value = True)

# insert data from csv into Sqlite     
def insert_data_into_sqlite(table_name, db_path, **kwargs):
    
    import sqlite3

    conn = sqlite3.connect(db_path)
    
    cur = conn.cursor()
    
    inserted_file = open(csv_file_path)
    
    rows = []
    for idx, row in enumerate(csv.reader(inserted_file)):
        # first row is column list
        if not idx:       
            cols = row
        
        else:
            rows.append(row)

    cur.executemany("INSERT INTO {} VALUES ({})".format(table_name, ','.join(['?' for _ in range(len(cols))])), rows)
    conn.commit()
    
    # check rowcount of tsable
    cur.execute("SELECT COUNT(*) FROM {}".format(table_name))   
    overall_row_count = cur.fetchall()[0][0]
    
    conn.commit()
    # save overall rwocount of this table
    kwargs['ti'].xcom_push(key = 'overall_rowcount', value = overall_row_count)
    kwargs['ti'].xcom_push(key = 'table_name', value = table_name)

# Create DAG
with DAG(dag_id = dag_id, default_args = default_args) as dag:
    
    send_slack_startng_message = SlackAPIPostOperator(
                                              task_id = 'slack_starting_message',
                                              token = os.environ['SLACK_BOT_TOKEN'],
                                              text = "",
                                              channel = 'log-test',
                                              username = "AIRFLOW_USER ({})".format(dag_id),
                                              attachments = [
                                                          {
                                                              "fallback": "Start ETL job: {}".format(dag_id),
                                                              "color": "#2eb886",
                                                              "author_name": "Shifty Hsu",
                                                              "title": "Start process: {}".format(dag_id)                                                   
                                                          }
                                                      ]
                                             )
    
    # create table `power_duration` at Sqlite
    create_table = SqliteOperator(task_id = 'create_table', 
                              sqlite_conn_id = 'airflow_sqlite', 
                              sql = sqlite_script)
    
    
    get_raw_bigquery_data = BranchPythonOperator(
        task_id='get_bigquery_data_as_df',
        op_args = ['2020-06-15'],
        python_callable = pull_raw_data_from_bq,
        provide_context = True
    )
    
    save_df_as_csv = PythonOperator(
        task_id = 'save_as_csv',
        python_callable = save_df_to_csv,
        op_kwargs={'csv_path': csv_file_path, 
                   'is_keep_index': False},
        provide_context = True
    )
    
    
    check_csv_exist = FileSensor(task_id = "csv_file_sensor", 
                         poke_interval = 3, 
                         filepath = csv_file_path)
    
    # do nothing
    do_nothing = DummyOperator(task_id = 'do_nothing')
    
    # insert data into Sqlite   
    store_data = PythonOperator(
        task_id = 'store_data_in_sqlite',
        python_callable = insert_data_into_sqlite,
        op_kwargs={'table_name': 'power_duration', 
                   'db_path':  os.environ['airflow_db_path']},
        provide_context = True)
    
    #send message via slack

    send_slack_ending_message = SlackAPIPostOperator(
                                              task_id = 'slack_ending_message',
                                              token = os.environ['SLACK_BOT_TOKEN'],
                                              text = "",
                                              channel = 'log-test',
                                              username = "AIRFLOW_USER ({})".format(dag_id),
                                              attachments = [
                                                          {
                                                              "fallback": "Finish ETL job: {}".format(dag_id),
                                                              "color": "#2eb886",
                                                              "author_name": "Shifty Hsu",
                                                              "title": "Succeeded to finish process: {}".format(dag_id),
                                                              "text": '''Overall rowcount of {{task_instance.
                                                              xcom_pull('store_data_in_sqlite', key='table_name') }}: 
                                                              {{ task_instance.
                                                              xcom_pull('store_data_in_sqlite',key='overall_rowcount') }}'''                                                              
                                                          }
                                                      ]
                                             )
    

send_slack_startng_message >> create_table >> get_raw_bigquery_data >> [save_df_as_csv, do_nothing]
    
save_df_as_csv >> check_csv_exist >> store_data >> send_slack_ending_message