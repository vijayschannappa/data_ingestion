from sysconfig import get_scheme_names
import psycopg2
import pandas as pd
from io import BytesIO,StringIO
import boto3,gzip
import configparser
import logging
import datetime
import time
import json
import jaydebeapi, os

parser = configparser.ConfigParser()
# parser.read('src/dags/PM_DataIngestion/creds.ini')
parser.read('creds.ini')

username = parser.get('AT_DB','username')
pwd = parser.get('AT_DB','pwd')
source = parser.get('AT_DB','source_details')
source= json.loads(source)
print(source)
jdbc_driver_name = parser.get('AT_DB','jdbc_driver_name')
jdbc_driver_loc = parser.get('AT_DB','jdbc_driver_loc')
connection_string = parser.get('AT_DB','connection_string')

test_schema = 'public'

prefix = 'at'


def get_source_connection(db):
    url = '{0}:user={1};password={2}'.format(connection_string, username,pwd)
    print("Connection String: " + connection_string)
    print(url)
    con = jaydebeapi.connect(jdbc_driver_name, connection_string, {'user': username, 'password': pwd},jars=jdbc_driver_loc)
    return con

def get_fffdp_connection():
    username = parser.get('FFDP_DB','username')
    database = parser.get('FFDP_DB','database')
    pwd = parser.get('FFDP_DB','pwd')
    host = parser.get('FFDP_DB','host')
    con = psycopg2.connect(user = username,
                                    password = pwd,
                                    host = host,
                                    port = "5439",
                                    database = database)
    return con


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

def get_schema_details(table_name,con,db,type=None):
    if type == 'source':
        query = f"""SELECT ''|| column_name || ' ' || udt_name ||
    case when is_nullable = 'YES' then ' NULL' else ' NOT NULL' end as column_expr
    FROM information_schema.columns
    WHERE table_schema || '.' || table_name = '{test_schema}.{table_name}'
    ORDER BY ordinal_position;"""
    elif type == 'ffdp':
        query = f"""SELECT ''|| column_name || ' ' || udt_name || 
    case when is_nullable = 'YES' then ' NULL' else ' NOT NULL' end as column_expr
    FROM information_schema.columns
    WHERE table_schema || '.' || table_name = '{prefix}_{test_schema}_{db}.{table_name}'
    ORDER BY ordinal_position;"""
    df = pd.read_sql(query,con)
    df['column_expr'] = df['column_expr'].str.replace('uuid','varchar')
    df['column_expr']= df['column_expr'].str.replace("jsonb","varchar")
    df['column_expr']= df['column_expr'].str.replace('USER-DEFINED','varchar')
    df['column_expr']= df['column_expr'].str.replace('text','varchar')
    df['column_expr']= df['column_expr'].str.replace('timestamptz','timestamp')
    # df['column_expr']= df['column_expr'].str.replace('varchar(255)','varchar')
    return df


def detect_schema_changes():
    db = 'core'
    s_con = get_source_connection(db)
    ffdp_con = get_fffdp_connection()
    test_tables = fetch_tables(s_con,db)
    for table in test_tables:
        source_schema_fr = get_schema_details(table,s_con,db,type='source')
        ffdp_schema_fr = get_schema_details(table,ffdp_con,db,type='ffdp')
        if source_schema_fr.equals(ffdp_schema_fr):
            print(f'no schema changes in {table}')
        else:
            print(f'source schema for {table} is \n: {source_schema_fr}')
            print(f'ffdp schema {table} is \n: {ffdp_schema_fr}')


if __name__ == "__main__":
    detect_schema_changes()