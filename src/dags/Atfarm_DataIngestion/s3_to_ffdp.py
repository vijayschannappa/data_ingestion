# from turtle import clear
import psycopg2
import pandas as pd
from io import BytesIO,StringIO
import boto3,gzip
import configparser
import logging
import datetime
import time
import json
import concurrent.futures as cf
# from airflow.hooks.base_hook import BaseHook


flag = 'local'

# source = parser.get('','source_details')
# source= json.loads(source)

folder_path = "atfarm"
test_schema = 'public'


dt = str(datetime.date.today() - datetime.timedelta(days=0))
# print(dt)


def get_db_connection():
    global bucket
    if flag == 'local':
        parser = configparser.ConfigParser()
        parser.read('/Users/vijaychannappa/Desktop/ydf/atfarm_di/creds.ini')
        username = parser.get('FFDP','username')
        database = parser.get('FFDP','database')
        pwd = parser.get('FFDP','pwd')
        host = parser.get('FFDP','host')
        bucket = parser.get('s3_details','bucket')     
    elif flag == 'cloud':
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
    return con


def get_aws_creds():
    cred = boto3.Session().get_credentials()
    aws_access_key_id=cred.access_key
    aws_secret_access_key=cred.secret_key
    aws_session_token=cred.token
    return aws_access_key_id, aws_secret_access_key, aws_session_token


def initiate_s3_connection():
    if flag == 'local':
        key,secret,token = get_aws_creds()
        client = boto3.client('s3',aws_access_key_id = key,
        aws_secret_access_key = secret,
        aws_session_token = token)
        s3 = boto3.resource('s3')
        my_bucket = s3.Bucket('ffdp-data-general-stage')
        return my_bucket,client,key,secret,token
    elif flag == 'cloud':
        s3_resource = boto3.resource('s3')
        client = boto3.client('s3')
        bucket = 'ffdp-data-general-stage'
        my_bucket = s3_resource.Bucket(bucket)
        return my_bucket,client


def exec_push_data_to_db(bucket,file_key,cursor,db,key=None,secret=None,token=None):
    s3_url = f's3://{bucket}/{file_key}'
    table = file_key.rsplit('/',1)[-1].rsplit('.',1)[0].split('_',1)[-1]
    if flag == 'local':
        query = f"""COPY at_{test_schema}_{db}.{table} FROM '{s3_url}' FORMAT as CSV dateformat 'auto' timeformat 'auto' IGNOREHEADER 1 credentials 'aws_access_key_id={key};aws_secret_access_key={secret};token={token}';"""
    if flag =='cloud':
        #query = f"""COPY at_{test_schema}_{db}.{table} FROM '{s3_url}' FORMAT as CSV dateformat 'auto' timeformat 'auto' IGNOREHEADER 1 iam_role 'arn:aws:iam::504749939156:role/ffdp-stage';"""
        query = f"""COPY at_{test_schema}_{db}.{table} FROM '{s3_url}' FORMAT as CSV dateformat 'auto' timeformat 'auto' IGNOREHEADER 1 iam_role 'arn:aws:iam::504749939156:role/ffdp-farm-field-stage';"""
    print(f"Pushing data into {db}.{table} from {s3_url}")
    print(f"Query for pushing:\n {query}")
    try:
        cursor.execute(query)
        print('query executed successfully')
    except Exception as e:
        cursor.execute("ROLLBACK")
        print(f"Query could not be executed due to {e} \n and hence {table} not updated/loaded")


def push_data_to_db(con,cursor,db):
    if flag =='local':
        bucket_connection, client,key,secret,token = initiate_s3_connection()
    elif flag == 'cloud':
        bucket_connection,client = initiate_s3_connection()
    print(bucket_connection)
    with cf.ThreadPoolExecutor() as exe:
        task_lst=[]
        for file in bucket_connection.objects.all():
            file_key = file.key
            # if folder_path in file_key and (str(datetime.date.today())) in file_key and 'manifest' in file_key:
            if folder_path in file_key and db in file_key and dt in file_key:
                print(file_key)
                if flag == 'local':
                    task_lst.append(exe.submit(exec_push_data_to_db,bucket=bucket,file_key=file_key,key=key,secret=secret,token=token,cursor=cursor,db=db))
                elif flag =='cloud':
                    task_lst.append(exe.submit(exec_push_data_to_db,bucket=bucket,file_key=file_key,cursor=cursor,db=db))      
    con.commit()
    cursor.close()
    con.close()



def load_atfarm_data():
    db = 'core'
    ffdp_con = get_db_connection()
    ffdp_cursor = ffdp_con.cursor()
    push_data_to_db(ffdp_con,ffdp_cursor,db)


def s3_to_ffdp_func():
    load_atfarm_data()

if __name__ == "__main__":
    s3_to_ffdp_func()
