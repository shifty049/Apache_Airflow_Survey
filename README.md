# Apache Airflow - Learning Note

## Airflow is a `Workflow Management System` developed by Python,  and was created and opened source by Airbnb


## DAG (`Directed Acyclic Graph`)
![DAG](https://i.imgur.com/BAgo7VR.png)

[Reference](https://medium.com/kriptapp/guide-what-is-directed-acyclic-graph-364c04662609)

## DAG Application: `prerequisite structure of courses`
![prerequisite structure of courses](https://i.imgur.com/NlhNUg5.png)

[Reference](https://www.chegg.com/homework-help/questions-and-answers/set-required-courses-degree-directed-acyclic-graph-shows-prerequisite-structure-courses-jo-q17273583)

## Constitution of Airflow

1. Webserver - GUI for operating, managing and monitoring
2. Scheduler - responsible for scheduling
3. worker - responsible for executing DAG tasks

## Operator - used for execute each kind of task

- SqliteOperator
- PythonOperator
- BranchPythonOperator 
- SlackAPIPostOperator
- BashOperator
- FileSensor
- DummyOperator

## data transfer

- 3rd party storage solution
- XCOM

    

## requirements

1.python 3.7
2.apache-airflow==2.1.0


## cmd

1. **`launch webserver:` nohup airflow webserver -p 8080 &> ~/airflow_server_log.txt &**
2. **`launch schedule:` nohup airflow scheduler &> ~/airflow_schedule_log.txt &**
