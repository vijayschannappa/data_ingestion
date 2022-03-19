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

parser = configparser.ConfigParser()
# parser.read('dags/Atfarm_DataIngestion/creds.ini')
parser.read('creds.ini')


username = parser.get('FFDP','username')
database = parser.get('FFDP','database')
pwd = parser.get('FFDP','pwd')
host = parser.get('FFDP','host')

# source = parser.get('','source_details')
# source= json.loads(source)

bucket = parser.get('s3_details','bucket')

folder_path = "farm_cherry"
test_schema = 'public'
prefix='fc'

dt = str(datetime.date.today() - datetime.timedelta(days=0))
print(dt)


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


def exec_push_data_to_db(bucket,file_key,key,secret,token,cursor,db):
    s3_url = f's3://{bucket}/{file_key}'
    table = file_key.rsplit('/',1)[-1].rsplit('.',1)[0].split('_',1)[-1]
    query = f"""COPY {prefix}_{test_schema}_{db}.{table} FROM '{s3_url}' FORMAT as CSV dateformat 'auto' timeformat 'auto' IGNOREHEADER 1 credentials 'aws_access_key_id={key};aws_secret_access_key={secret};token={token}';"""
    print(f"Pushing data into {db}.{table} from {s3_url}")
    print(f"Query for pushing:\n {query}")
    try:
        cursor.execute(query)
        print('query executed successfully')
    except Exception as e:
        cursor.execute("ROLLBACK")
        print(f"Query could not be executed due to {e} \n and hence {table} not updated/loaded")


def push_data_to_db(con,cursor,db):
    key,secret,token = get_aws_creds()
    bucket_connection, client = initiate_s3_connection(key,secret,token)
    print(bucket_connection)
    with cf.ThreadPoolExecutor() as exe:
        task_lst=[]
        for file in bucket_connection.objects.all():
            file_key = file.key
            # if folder_path in file_key and (str(datetime.date.today())) in file_key and 'manifest' in file_key:
            if folder_path in file_key and db in file_key and dt in file_key:
                print(file_key)
                print(db)
                task_lst.append(exe.submit(exec_push_data_to_db,bucket=bucket,file_key=file_key,key=key,secret=secret,token=token,cursor=cursor,db=db))
    con.commit()
    cursor.close()
    con.close()



def load_atfarm_data(db):
    ffdp_con = get_db_connection()
    ffdp_cursor = ffdp_con.cursor()
    push_data_to_db(ffdp_con,ffdp_cursor,db)


def s3_to_ffdp_func():
    dbs = ['farmer_service','auth_service','address_service']
    for db in dbs:
        load_atfarm_data(db)

if __name__ == "__main__":
    s3_to_ffdp_func()
