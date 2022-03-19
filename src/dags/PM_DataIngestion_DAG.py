import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

from PM_DataIngestion.db_to_s3 import db_to_s3_func
from PM_DataIngestion.s3_to_ffdp import s3_to_ffdp_func
from PM_DataIngestion.create_tables import create_drop_tables_func
import pendulum

local_tz = pendulum.timezone('Asia/Calcutta')
#local_tz = pendulum.timezone("Europe/Amsterdam")


"""
This DAG is for transferring data from Professional Markets Legacy DB to FFDP Redshift Cluster
The schedule interval is weekly
The start date is 2022, 1, 30, 6, 0, 0
"""
default_args = {
    'owner': 'Animesh',
    'depends_on_past': False,
    #'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    #'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

dag = DAG(
    'PM_Data_Ingestion_one_time_Load',
    default_args=default_args,
    description='New dag for Legacy PM to Redshift data transfer',
    #schedule_interval='@daily',
    start_date=datetime.datetime(2022, 2, 14, 6, 0, 0,tzinfo=local_tz)
)


   
t1 = DummyOperator(task_id = 'start',dag=dag)

t2 = PythonOperator(
        task_id='pm_to_s3_datadump',
        python_callable=db_to_s3_func,
        dag=dag
    )

t3 = PythonOperator(
        task_id='refresh_existing_ffdp_schema',
        python_callable=create_drop_tables_func,
        dag=dag
    )

t4 = PythonOperator(
        task_id='s3_to_ffdp',
        python_callable=s3_to_ffdp_func,
        dag=dag
    )

t5 = DummyOperator(task_id = 'end',dag=dag)

#t2 >> t3 >> t4 >> t5

t1 >> [t2,t3] >> t4 >> t5