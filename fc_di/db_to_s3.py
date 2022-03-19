from numpy import empty
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



username = parser.get('FARM_CHERRY','username')
pwd = parser.get('FARM_CHERRY','pwd')
source = parser.get('FARM_CHERRY','source_details')
bucket = parser.get('s3_details','bucket')
source= json.loads(source)

folder_path = "farm_cherry"
test_schema = 'public'
prefix="fc"


# logging.basicConfig(filename="db_to_s3.log",
#                     format='%(asctime)s %(message)s',
#                     filemode='w',level=print)

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

print(f'Job started at {str(datetime.datetime.now())}')


def fetch_tables(con,db,all=False):
    if all:
        fetch_schema_and_tables = """SELECT table_schema,table_name
        FROM information_schema.tables where table_schema != 'information_schema'
        ORDER BY table_schema,table_name;"""
        df = pd.read_sql(fetch_schema_and_tables,con)
        print(f'union of tables and schema:\n {df.head()}')
        distinct_schemas = list(df['table_schema'].unique())
        print(f'DISTINCT SCHEMAS:\n{distinct_schemas}')
        tables = list(df[df['table_schema']==test_schema]['table_name'])
        print(f"TEST TABLES:\n{tables}")
    else:
        tables = source.get(db).get('tables')
    return tables



def get_tabular_frame(con,table,ffdp_data_metrics,db,client):
    if ffdp_data_metrics['row_count'][0] == 0:
        query = """SELECT * FROM "{}".{}""".format(test_schema,table)
        #count_query = """SELECT COUNT(*) FROM "{}".{};""".format(test_schema,table)
    else:
        # latest_created = str(datetime.datetime.strptime(ffdp_data_metrics['latest_created'][0],'%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours=5,minutes=29,seconds=59))
        # latest_updated = str(datetime.datetime.strptime(ffdp_data_metrics['latest_updated'][0],'%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours=5,minutes=29,seconds=59))
    
        latest_created = str(datetime.datetime.strptime(ffdp_data_metrics['latest_created'][0],'%Y-%m-%d %H:%M:%S') + datetime.timedelta(seconds=1))
        latest_updated = str(datetime.datetime.strptime(ffdp_data_metrics['latest_updated'][0],'%Y-%m-%d %H:%M:%S') + datetime.timedelta(seconds=1))    
        query = """SELECT * FROM "{}".{} where created_at >= '{}' or updated_at >= '{}' """.format(test_schema,table,latest_created,latest_updated)
        # count_query = count_query = """SELECT COUNT(*) FROM "{}".{}  where created > '{}' or updated > '{}';""".format(test_schema,table,latest_created,latest_updated)
    print(f"Query:\n{query}")
    cursor = con.cursor()
    # cursor.execute(count_query)
    df = pd.read_sql(query,con)
    print(f"query results:\n{df.head()}")
    print(f"total number of rows extracted from {table}: {len(df)}")
    push_tabluar_frame_to_s3(df,table,db,client)
    # df.to_csv(f'sample_{table}.csv',index=False)
    return df


def push_tabluar_frame_to_s3(table_frame,table,db,client):
    csv_buf = StringIO()
    # out_buf = BytesIO()
    table_frame.to_csv(csv_buf, header=True, index=False)
    # table_frame.to_parquet(out_buf, index=False)
    csv_buf.seek(0)
    client.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=f'{folder_path}/{test_schema}/{db}/{str(datetime.date.today())}_{table}.csv')
    print(f'pushed {table} data to S3')

def clear_bucket(client,db):
    prefix = f'{folder_path}/{test_schema}/{db}'
    response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    file_count = response.get('KeyCount')
    if file_count > 0:
        print(f"Found {file_count} files inside the {prefix} folder")
        for object in response['Contents']:
            print(f"Deleting {object['Key']}")
            client.delete_object(Bucket=bucket, Key=object['Key'])
    else:
        print(f"Found {file_count} files inside the {prefix} folder")
        
def initiate_s3_connection():
    cred = boto3.Session().get_credentials()
    client = boto3.client(
    's3',
    aws_access_key_id=cred.access_key,
    aws_secret_access_key=cred.secret_key,
    aws_session_token=cred.token)
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucket)
    return my_bucket,client


def get_source_connection(db):
    host = source.get(db).get('host')
    con = psycopg2.connect(user = username,
                                    password = pwd,
                                    host = host,
                                    port = "5432",
                                    database = db)
    return con

def get_FFDP_connection():
    username = parser.get('FFDP','username')
    database = parser.get('FFDP','database')
    pwd = parser.get('FFDP','pwd')
    host = parser.get('FFDP','host')
    ffdp_con = psycopg2.connect(user = username,
                                    password = pwd,
                                    host = host,
                                    port = "5439",
                                    database = database)
    return ffdp_con


def get_table_metrics(table_name,con,db,type):
    if type =='ffdp':
        query = f"""select date_trunc('second',max(created_at)) as latest_created, date_trunc('second',max(updated_at)) as latest_updated, count(*) as row_count from {prefix}_{test_schema}_{db}.{table_name};"""
        # query = f"""select date_trunc('second',dateadd(minute,-330,max(created))) as latest_created, date_trunc('second',dateadd(minute,-330,max(updated))) as latest_updated, count(*) as row_count from {prefix}_{test_schema}_{db}.{table_name};"""
    elif type == 'source':
        query = f"""select date_trunc('second',max(created_at)) as latest_created, date_trunc('second',max(updated_at)) as latest_updated, count(*) as row_count from {test_schema}.{table_name};"""
    print(query)
    df = pd.read_sql(query,con)
    df['latest_created'] = df['latest_created'].astype(str)
    df['latest_updated'] = df['latest_updated'].astype(str)
    df['latest_created'] = df['latest_created'].apply(lambda x: x.split('+')[0] if '+' in x else x)
    df['latest_updated'] = df['latest_updated'].apply(lambda x: x.split('+')[0] if '+' in x else x)
    return df


def is_data_changes_there(table,s_con,ffdp_con,db):
    source_data_metrics = get_table_metrics(table,s_con,db,type='source')
    ffdp_data_metrics= get_table_metrics(table,ffdp_con,db,type='ffdp')
    # source_data_metrics.to_csv('sdm.csv',index=False)
    # ffdp_data_metrics.to_csv('fdm.csv',index=False)
    if source_data_metrics.equals(ffdp_data_metrics):
        print(f'Source matches with destination, hence no update is needed for {table}')
        print(f'source metrics for {table} is: \n {source_data_metrics}')
        print(f'ffdp metrics for {table} is: \n {ffdp_data_metrics}')
        empty = pd.DataFrame()
        return empty
    else:
        print(f'Source mismatches with destination, hence update is needed for {table}')
        print(f'source metrics for {table} is: \n {source_data_metrics}')
        print(f'ffdp metrics for {table} is: \n {ffdp_data_metrics}')
        return ffdp_data_metrics


def fetch_push_data(s_con,ffdp_con,test_tables,db,client,force_load):
    changed_data_metrics_final ={}
    for table in test_tables:
        changed_data_metrics_final[table] = is_data_changes_there(table,s_con,ffdp_con,db)
    print(changed_data_metrics_final)
    # Parallelisation for gettables
    print(changed_data_metrics_final.items())
    with cf.ThreadPoolExecutor() as exe:
        table_frame={}
        table_frame1={}
        for table,table_frame_metrics in changed_data_metrics_final.items():
            if not table_frame_metrics.empty or force_load:
                try:
                    # table_frame[table] = table_frame_metrics
                    table_frame[table] = exe.submit(get_tabular_frame,con=s_con,table=table,ffdp_data_metrics=table_frame_metrics,db=db,client=client)
                    #table_frame = get_tabular_frame(s_con,table,changed_data_metrics)
                except Exception as e:
                    logging.error(f"Unable to read data from the legacy db due to:{e}")
        # print(table_frame)
        for task,table in zip(cf.as_completed(table_frame.values()),table_frame.keys()):
            task.result()
    s_con.commit()
    # cur.close()
    s_con.close()
    ffdp_con.commit()
    ffdp_con.close()
    print(f'Job ended at {str(datetime.datetime.now())}')




def fetch_and_push_db_data(db,force_load):
    s_con = get_source_connection(db = db)
    ffdp_con = get_FFDP_connection()
    test_tables = fetch_tables(s_con,db)
    my_bucket, client = initiate_s3_connection()
    # clear_bucket(client,db)
    fetch_push_data(s_con,ffdp_con,test_tables,db,client,force_load)

def db_to_s3_func(force_load=False):
    dbs = ['farmer_service','auth_service','address_service']
    for db in dbs:
        fetch_and_push_db_data(db,force_load)


if __name__ == "__main__":
    start = time.time()
    db_to_s3_func(force_load=False)
    end = time.time()    
    print(f'Final Time is {end-start}')
