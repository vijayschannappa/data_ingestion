import psycopg2
import pandas as pd
from io import StringIO
import boto3
import configparser
import concurrent.futures as cf
import time
import datetime

from airflow.hooks.base_hook import BaseHook

#flag = 'cloud' #cloud for airflow
flag = 'local'

if flag == 'local':
    parser = configparser.ConfigParser()
    parser.read('dags/PM_DataIngestion/creds.ini')
    username = parser.get('FFDP_DB','username')
    database = parser.get('FFDP_DB','database')
    pwd = parser.get('FFDP_DB','pwd')
    host = parser.get('FFDP_DB','host')

if flag == 'cloud':
    parser = BaseHook.get_connection('ffdp_redshift')
    username = parser.login
    database = parser.schema
    pwd = parser.password
    host = parser.host
    bucket= 'ffdp-data-general-stage'



folder_path = "professional-market/activecampaign_production/"




def get_db_connection():
    con = psycopg2.connect(user = username,
                                    password = pwd,
                                    host = host,
                                    port = "5439",
                                    database = database)
    return con

def get_aws_creds():
    cred = boto3.Session().get_credentials()
    aws_access_key_id=cred.access_key
    aws_secret_access_key=cred.secret_key
    aws_session_token=cred.token
    return aws_access_key_id, aws_secret_access_key, aws_session_token

def initiate_s3_connection(key,secret,token):
    client = boto3.client('s3',aws_access_key_id = key,
    aws_secret_access_key = secret,
    aws_session_token = token)
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket('ffdp-data-general-stage')
    return my_bucket,client


def exec_push_data_to_db(bucket,file_key,key,secret,token,cursor,client):

    obj = client.get_object(Bucket=bucket, Key=file_key)
    s3_url = f's3://{bucket}/{file_key}'
    # schema= file_key.split('/')[-2]
    # table = file_key.split('/')[-1].split('.')[0]
    schema= file_key.split('/')[-2]
    table = file_key.split('/')[-1].split('_',1)[-1].split('manifest')[0]
    # query = f"""COPY pm_{schema}.{table} FROM '{s3_url}' FORMAT CSV credentials 'aws_access_key_id={key};aws_secret_access_key={secret};token={token}' delimiter ',' IGNOREHEADER 1;"""
    query = f"""COPY pm_{schema}.{table} FROM '{s3_url}' FORMAT as parquet credentials 'aws_access_key_id={key};aws_secret_access_key={secret};token={token}' manifest;"""
    print(f"Pushing data into {schema}.{table} from {s3_url}")
    print(f"Query for pushing:\n {query}")
    try:
        cursor.execute(query)
        print('query executed successfully')
    except Exception as e:
        cursor.execute("ROLLBACK")
        print(f"Query could not be executed due to {e} \n and hence {table} not updated/loaded")



# def push_data_to_db(cursor):
#     bucket = 'ffdp-data-general-stage'
#     key,secret,token = get_aws_creds()
#     bucket_connection, client = initiate_s3_connection(key,secret,token)
#     print(bucket_connection)
    # Setting up tasks for multithreading
    # with cf.ThreadPoolExecutor() as exe:
    #     task_lst=[]
    #     for file in bucket_connection.objects.all():
    #         file_key = file.key
    #         if folder_path in file_key:
    #             task_lst.append(exe.submit(exec_push_data_to_db,bucket=bucket,file_key=file_key,key=key,secret=secret,token=token,cursor=cursor,client=client))
            # if folder_path in file_key:
            #     obj = client.get_object(Bucket=bucket, Key=file_key)
            #     s3_url = f's3://{bucket}/{file_key}'
            #     print(s3_url)
            #     schema= file_key.split('/')[-2]
            #     table = file_key.split('/')[-1].split('.')[0]
            #     query = f"""COPY pm_{schema}.{table} FROM '{s3_url}' FORMAT CSV credentials 'aws_access_key_id={key};aws_secret_access_key={secret};token={token}' delimiter ',' IGNOREHEADER 1;"""
            #     print(query)
            #     try:
            #         cursor.execute(query)
            #         print('query executed')
            #     except Exception as e:
            #         cursor.execute("ROLLBACK")
            #         print(query)
            #         print(e)
        #     #         print(f'{table} not loaded')
        # for tf in cf.as_completed(task_lst):
        #     tf.result()

def push_data_to_db(cursor):
    bucket = 'ffdp-data-general-stage'
    key,secret,token = get_aws_creds()
    bucket_connection, client = initiate_s3_connection(key,secret,token)
    print(bucket_connection)
    with cf.ThreadPoolExecutor() as exe:
        task_lst=[]
        for file in bucket_connection.objects.all():
            file_key = file.key
            if folder_path in file_key and (str(datetime.date.today())) in file_key and 'manifest' in file_key:
                task_lst.append(exe.submit(exec_push_data_to_db,bucket=bucket,file_key=file_key,key=key,secret=secret,token=token,cursor=cursor,client=client))

        for tf in cf.as_completed(task_lst):
            tf.result()


def s3_to_ffdp_func():
    start = time.time()
    con = get_db_connection()
    cursor = con.cursor()
    # key,secret,token = get_aws_creds()
    # bucket_connection, client = initiate_s3_connection(key,secret,token)
    print('i am here')
    push_data_to_db(cursor)
    con.commit()
    cursor.close()
    con.close()
    end = time.time()    
    print(f'Final Time is {end-start}')

if __name__ == "__main__":
    s3_to_ffdp_func()