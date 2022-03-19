import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import psycopg2
import pandas as pd
from io import BytesIO,StringIO
import boto3,gzip
import configparser
import logging
import datetime
import concurrent.futures as cf
import time
from airflow.hooks.base_hook import BaseHook
from Atfarm_DataIngestion.db_to_s3 import db_to_s3_func
from Atfarm_DataIngestion.s3_to_ffdp import s3_to_ffdp_func
from Atfarm_DataIngestion.create_tables import create_drop_tables_func
from Atfarm_DataIngestion.farm_profile_DI.ffdp_to_farm_profile import run_ffdp_to_farm_profile
import pendulum

local_tz = pendulum.timezone('Asia/Calcutta')
#local_tz = pendulum.timezone("Europe/Amsterdam")


"""
This DAG is for transferring data from Atfarm Legacy DB Legacy DB to FFDP Redshift Cluster
The schedule interval is weekly
The start date is 2022, 1, 30, 6, 0, 0
"""
default_args = {
    'owner': 'Animesh',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
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
    'Atfarm_Data_Ingestion_one_time_Load',
    default_args=default_args,
    description='New dag for Legacy Atfarm Core DB to Redshift data transfer',
    schedule_interval='@daily',
    #schedule_interval='0 19 * * 0-5',
    start_date=datetime.datetime(2022, 3, 15, 6, 0, 0,tzinfo=local_tz)
)



def check_Atfarm():
    parser = BaseHook.get_connection('atfarm')
    username = parser.login
    database = parser.schema
    pwd = parser.password
    host = parser.host
    bucket= 'ffdp-data-general-stage'
    print(username,database,pwd,host,bucket)
    con = psycopg2.connect(user = username,
                                    password = pwd,
                                    host = host,
                                    #port = "5439",
                                    database = database)
                                    
    cur = con.cursor()
    cur.execute('SELECT version()')
    print(cur.fetchone()[0])
    print('Atfarm_connection_done')
    return 0

def check_FFDP_redshift():
    parser = BaseHook.get_connection('ffdp_redshift')
    username = parser.login
    database = parser.schema
    pwd = parser.password
    host = parser.host
    bucket= 'ffdp-data-general-stage'
    print(username,database,pwd,host)
    con = psycopg2.connect(user = username,
                                    password = pwd,
                                    host = host,
                                    port = "5439",
                                    database = database)
    
    cur = con.cursor()
    cur.execute('SELECT version()')
    print(cur.fetchone()[0])
    print('FFDP_redshift_connection')
    return 0


def check_FFDP_s3():

    s3_resource = boto3.resource('s3')
    bucket = 'ffdp-data-general-stage'
    my_bucket = s3_resource.Bucket(bucket)
    print("Here")
    print(my_bucket)
    for obj in my_bucket.objects.all():
        print(obj)
    # for bucket in s3_resource.buckets.all():
    #     print(bucket.name)
    #return my_bucket,s3_client
    print('FFDP_s3_connection')
    return 0
    

# DAGS for chekcing connection to various dbs

t1 = DummyOperator(task_id = 'start_data_ingestion_Atfarm',dag=dag)


t2 = PythonOperator(
        task_id='check_FFDP_redshift_connection',
        python_callable=check_FFDP_redshift,
        dag=dag
    )



t3 = PythonOperator(
        task_id='check_Atfarm_connection',
        python_callable=check_Atfarm,
        dag=dag
    )

t4  = PythonOperator(
        task_id='check_FFDP_s3_connection',
        python_callable=check_FFDP_s3,
        dag=dag
    )

#t5 = DummyOperator(task_id = 'end',dag=dag)

#t1 >> [t2,t3,t4] >> t5


# Tasks defined for Atfarm connection

#s1 = DummyOperator(task_id = 'start_atfarm',dag=dag)

s2 = PythonOperator(
        task_id='update_atfarm_existing_schema_ffdp',
        python_callable=create_drop_tables_func,
        dag=dag
    )

s3 = PythonOperator(
        task_id='atfarm_to_s3_datadump',
        python_callable=db_to_s3_func,
        dag=dag
    )


s4 = PythonOperator(
        task_id='s3_to_ffdp_legacy_structure',
        python_callable=s3_to_ffdp_func,
        dag=dag
    )

s5 = PythonOperator(
        task_id='ffdp_legacy_structure_to_farmerprofile_ffdp',
        python_callable=run_ffdp_to_farm_profile,
        dag=dag
    )

s6 = DummyOperator(task_id = 'end_data_ingestion_Atfarm',dag=dag)

t1 >> [t2,t3,t4] >> s2 >> s3 >>s4 >> s5 >> s6