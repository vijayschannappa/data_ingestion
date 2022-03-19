import psycopg2
import pandas as pd
import sys
import configparser
import json


parser = configparser.ConfigParser()
# parser.read('dags/PM_DataIngestion/creds.ini')
parser.read('/Users/vijaychannappa/Desktop/ydf/atfarm_di/creds.ini')


username = parser.get('FARM_CHERRY','username')
pwd = parser.get('FARM_CHERRY','pwd')
source = parser.get('FARM_CHERRY','source_details')
bucket = parser.get('s3_details','bucket')
source= json.loads(source)


test_schema = 'public'

prefix = 'fc'


def drop_table(table,ffdp_cursor,db):
    drop_query = f"""DROP TABLE IF EXISTS {prefix}_{test_schema}_{db}.{table};"""
    ffdp_cursor.execute(drop_query)
    print(f"Deleted existing table {table}")

def create_table_ffdp(ddl_statement,ffdp_cursor):
    ffdp_cursor.execute(ddl_statement)


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





def get_ddl_statement(con,table_name,db):
    #udt_type to get user defined data type name
    query = f"""SELECT ''|| column_name || ' ' || udt_name || 
coalesce('(' || character_maximum_length || ')', '') || 
case when is_nullable = 'YES' then ' NULL' else ' NOT NULL' end as column_expr
FROM information_schema.columns
WHERE table_schema || '.' || table_name = '{test_schema}.{table_name}'
ORDER BY ordinal_position;"""
    df = pd.read_sql(query,con)
    # print(df.head())
    newline = '\n'
    col_exp_ls = list(df["column_expr"])
    _ddl = f"""CREATE TABLE IF NOT EXISTS {prefix}_{test_schema}_{db}.{table_name} {newline} ({'{},'*(len(col_exp_ls)-1)}{{}}"""
    ddl = _ddl.format(*col_exp_ls)
    key_constraint_statement = get_key_constraint_statement(con,table_name)
    if key_constraint_statement:
        # print(key_constraint_statement)
        ddl = f"{ddl},{key_constraint_statement});"
    else:
        ddl = f"{ddl});"
    print(f'source ddl:{ddl}')
    corrected_ddl = check_for_dtypes(ddl)
    print(f'corrected_ddl:{corrected_ddl}')
    return corrected_ddl
        

def check_for_dtypes(ddl):
    if 'jsonb' in ddl:
        ddl = ddl.replace("jsonb","character varying(500)")
    if 'uuid' in ddl:
        ddl = ddl.replace('uuid','character varying(255)')
    if 'USER-DEFINED' in ddl:
        ddl = ddl.replace('USER-DEFINED','character varying(500)')
    if 'text' in ddl:
        ddl = ddl.replace('text','character varying(400)')
    if 'timestamptz' in ddl:
        ddl = ddl.replace('timestamptz','timestamp')
    if 'valid_gender' in ddl:
        ddl = ddl.replace('valid_gender','character varying(20)')
    return ddl

def get_key_constraint_statement(con,table_name):
    cons_frame_query = f"""WITH unnested_confkey AS (
    SELECT oid, unnest(confkey) as confkey
    FROM pg_constraint
    ),
    unnested_conkey AS (
    SELECT oid, unnest(conkey) as conkey
    FROM pg_constraint
    )
    select
    c.conname                   AS constraint_name,
    c.contype                   AS constraint_type,
    tbl.relname                 AS constraint_table,
    col.attname                 AS constraint_column,
    referenced_tbl.relname      AS referenced_table,
    referenced_field.attname    AS referenced_column,
    pg_get_constraintdef(c.oid) AS definition
    FROM pg_constraint c
    LEFT JOIN unnested_conkey con ON c.oid = con.oid
    LEFT JOIN pg_class tbl ON tbl.oid = c.conrelid
    LEFT JOIN pg_attribute col ON (col.attrelid = tbl.oid AND col.attnum = con.conkey)
    LEFT JOIN pg_class referenced_tbl ON c.confrelid = referenced_tbl.oid
    LEFT JOIN unnested_confkey conf ON c.oid = conf.oid
    LEFT JOIN pg_attribute referenced_field ON (referenced_field.attrelid = c.confrelid AND referenced_field.attnum = conf.confkey)
    where tbl.relname = '{table_name}';
    """
    cons_df = pd.read_sql(cons_frame_query,con)
    cons_df = cons_df[cons_df['definition'] != 'TRIGGER']
    cons_df = cons_df[~cons_df['definition'].str.contains("CHECK")]
    cons_df = cons_df[~cons_df['definition'].str.contains("FOREIGN KEY")]
    cons_df = cons_df[~cons_df['definition'].str.contains("row_version")]
    # print(f'cons_df for {table_name}:\n {cons_df.head()}')
    # cons_df.to_csv('cons_df.csv',index=False)
    if cons_df.empty:
        return None
    cons_df = cons_df.sort_values('definition')
    cons_df = cons_df.drop_duplicates(keep='last',subset=['constraint_column'])
    cons_df = cons_df.drop_duplicates(keep='last',subset=['constraint_name','definition'])
    condefs = list(cons_df['definition'])
    connames = list(cons_df['constraint_name'])
    merged_list = fetch_merged_condefs_connames_list(connames,condefs)
    format_cons_statement = f"{'CONSTRAINT {} {},'*(len(condefs)-1)} {'CONSTRAINT {} {}'}"
    cons_statement = format_cons_statement.format(*merged_list)
    return cons_statement

def fetch_merged_condefs_connames_list(connames,condefs):
    key_constraint_list = []
    for key_name, key_type in zip(connames,condefs):
        key_constraint_list.append(key_name)
        key_constraint_list.append(key_type)
    print(key_constraint_list)
    return key_constraint_list

def get_source_connection(db):
    host = source.get(db).get('host')
    con = psycopg2.connect(user = username,
                                    password = pwd,
                                    host = host,
                                    port = "5432",
                                    database = db)
    return con

def create_schema_if_not_exists(ffdp_cursor,db):
    query = f"""CREATE SCHEMA IF NOT EXISTS {prefix}_{test_schema}_{db};"""
    ffdp_cursor.execute(query)
    print(f'schema query:\n {query}')
    print('checked the schema')


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
    df['column_expr']= df['column_expr'].str.replace('valid_gender','varchar')
    return df

def is_schema_changes_there(table,s_con,ffdp_con,db,force_create):
    source_schema_fr = get_schema_details(table,s_con,db,type='source')
    ffdp_schema_fr = get_schema_details(table,ffdp_con,db,type='ffdp')
    if force_create:
        print(f'FORCE CREATING THE {table}')
        return True
    if source_schema_fr.equals(ffdp_schema_fr):
        print(f'no schema changes in {table}')
        return False
    else:
        print(f'schema changes exist in {table}')
        print(f'source schema for {table} is \n: {source_schema_fr}')
        print(f'ffdp schema {table} is \n: {ffdp_schema_fr}')
        return True

def fetch_source_schema_and_create_tables(tables,source_con,dest_con,dest_cursor,db,force_create=False):
    for table in tables:
        print(f'detecting schema changes in {table}')
        if is_schema_changes_there(table,source_con,dest_con,db,force_create):
            print(f'dropping and creating {table} with new schema')
            try:
                ddl_statement = get_ddl_statement(source_con,table,db)
                # print(ddl_statement)
                drop_table(table,dest_cursor,db)
                create_table_ffdp(ddl_statement,dest_cursor)
            except Exception as e:
                print(f'failed to create table {table} due to {e}')
                print(f'DDDL statement of table is:\n {ddl_statement}')
                dest_cursor.execute('ROLLBACK')
            else:
                print(f'successfully created new table {table}')
        else:
            continue
    dest_con.commit()
    dest_cursor.close()
    source_con.close()
    dest_con.close()

def create_fc_tables(db):
    s_con = get_source_connection(db = db)
    ffdp_con = get_FFDP_connection()
    tables = fetch_tables(s_con,db, all = False)
    ffdp_cursor = ffdp_con.cursor()
    create_schema_if_not_exists(ffdp_cursor,db)
    fetch_source_schema_and_create_tables(tables,s_con,ffdp_con,ffdp_cursor,db,force_create=True)

def create_drop_tables_func():
    dbs = ['farmer_service','auth_service','address_service']
    for db in dbs:
        create_fc_tables(db)

if __name__ == "__main__":
    create_drop_tables_func()