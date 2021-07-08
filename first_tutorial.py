from airflow.operators.python import PythonOperator
from airflow.models import DAG
from datetime import datetime, timedelta

# set up a dag id for this dag
dag_id = 'first_airflow_tutorial'

# argument used in DAG can be put together in default_args exclude dag_id
default_args = {
                'description': 'first airflow data pipeline job',
                'start_date': datetime(2021,6,1),
                'schedule_interval' : '30 9 * * *',
                'retries': 2,
                'retry_delay': timedelta(seconds = 20)
                }

def func1(x, ti):
    '''
    ti is used for interacting with XCOM
    '''
    # calculating the cubed x
    
    result = pow(x, 3)
    
    ti.xcom_push(key = 'result_of_func1', value = result)
    
    # if function has return , this return value will be automatically store in XCOM
    return result


def func2(y, z, ti):
    '''
    ti is used for interacting with XCOM
    '''
    
    result_of_func1 = ti.xcom_pull(key = 'result_of_func1', task_ids = 'func1_task_id')
    
    result = result_of_func1 * y + z
    
    ti.xcom_push(key = 'result_of_func2', value = result)
    
    return result

def func3(**check):
    '''
    **check is used for interacting with XCOM
    '''
  
    result_of_func1 = check['ti'].xcom_pull(key = 'result_of_func1', task_ids = 'func1_task_id')
    
    result = result_of_func1 ** 2
    
    check['ti'].xcom_push(key = 'result_of_func3', value = result)

def func4(compared_target, **kwargs):
    '''
    **check is used for interacting with XCOM
    '''
  
    result_of_func2 = kwargs['ti'].xcom_pull(key = 'result_of_func2', task_ids = 'func2_task_id')
    
    result_of_func3 = kwargs['ti'].xcom_pull(key = 'result_of_func3', task_ids = 'func3_task_id')
    
    result_sum = result_of_func2 + result_of_func3
    
    if result_sum > compared_target:
        
        compared_result = 'greater than'
    
    elif result_sum < compared_target:
        
        compared_result = 'less than'
    
    else:
        compared_result = 'equal to'
    
    result = 'the result sum {} is {} than the compared_target {}'.format(result_sum, 
                                                                                  compared_result, 
                                                                                  compared_target)    
    print(result)
    
    return result

# Create DAG

# equals to with DAG(dag_id = dag_id, 
#                    description = 'first airflow data pipeline job', 
#                    start_date = datetime(2021,6,1),
#                    schedule_interval = '30 9 * * *',
#                    retries = 2,
#                    retry_delay = timedelta(seconds = 20)
#                   )
with DAG(dag_id = dag_id, default_args = default_args) as dag:
    
    
    func1_operator = PythonOperator(       
        task_id = 'func1_task_id',
        python_callable = func1,
        op_kwargs = {'x': 3},
        provide_context = True
    )
    
    func2_operator = PythonOperator(       
        task_id = 'func2_task_id',
        python_callable = func2,
        op_kwargs={'y': 5, 
                   'z': 2},
        provide_context = True
    )
    
    func3_operator = PythonOperator(       
        task_id = 'func3_task_id',
        python_callable = func3,
        provide_context = True,
        trigger_rule = 'all_success'
    )
    
    func4_operator = PythonOperator(       
        task_id = 'func4_task_id',
        python_callable = func4,
        op_args = [172],
        provide_context = True,
        trigger_rule = 'all_success'
    )
    

## use PythonOperator object to set up ETL workflow

# func1_operator >> [func2_operator, func3_operator] >> func4_operator

## use up_stream / down_stream
func2_operator.set_upstream(func1_operator)
func1_operator.set_downstream(func3_operator)
func2_operator.set_downstream(func4_operator)
func4_operator.set_upstream(func3_operator)
