import psycopg2
import pandas as pd
import configparser
import logging
import datetime
from datetime import date
import time
import json
import boto3
# import jaydebeapi, os,sys
import concurrent.futures as cf
import sqlalchemy as sa
from io import BytesIO,StringIO
import sys,ast
from iso639 import languages

# from airflow.hooks.base_hook import BaseHook


class push_data_to_farm_profile:
    def __init__(self,source_schema,dest_schema,source_tables,dest_table,source_id,flag,bucket,truncate=False):
        self.source_schema = source_schema
        self.dest_schema = dest_schema
        self.source_tables = source_tables
        self.dest_table = dest_table
        self.source_id = source_id
        self.flag = flag
        self.bucket = bucket
        self.folder_path = 'farm_field_profile'
        self.truncate = truncate

    def fetch_push_data(self):
        self.ffdp_con = self.get_FFDP_connection()
        self.my_bucket,self.client = self.initiate_s3_connection()
        if self.truncate:
            self.tuncate_table()
        self.df_to_push = self.fetch_source_details()
        # push_user_details(engine,dest_schema,dest_table,df_to_push)
        if not self.df_to_push.empty:
            self.push_tabluar_frame_to_s3()
            self.copy_from_s3_to_ffdp()
            self.ffdp_con.commit()
            self.ffdp_con.close()
            self.clear_bucket()
    
    def clear_bucket(self):
        prefix = self.folder_path
        response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        file_count = response.get('KeyCount')
        if file_count > 0:
            print(f"Found {file_count} files inside the {prefix} folder")
            for object in response['Contents']:
                print(f"Deleting {object['Key']}")
                self.client.delete_object(Bucket=self.bucket, Key=object['Key'])
        else:
            print(f"Found {file_count} files inside the {prefix} folder")


    def tuncate_table(self):
        trunc_query = f"""Truncate {self.dest_schema}.{self.dest_table};"""
        cursor = self.ffdp_con.cursor()
        cursor.execute(trunc_query)
        print(f'truncated {self.dest_table}')
        cursor.close()

    def initiate_s3_connection(self):
        if self.flag == 'local':
            parser = configparser.ConfigParser()
            parser.read('/Users/vijaychannappa/Desktop/ydf/atfarm_di/creds.ini')
            cred = boto3.Session().get_credentials()
            client = boto3.client(
            's3',
            aws_access_key_id=cred.access_key,
            aws_secret_access_key=cred.secret_key,
            aws_session_token=cred.token)
            s3 = boto3.resource('s3')
            my_bucket = s3.Bucket(self.bucket)
        elif self.flag == 'cloud':
            s3_resource = boto3.resource('s3')
            client = boto3.client('s3')
            my_bucket = s3_resource.Bucket(self.bucket)
        return my_bucket,client

    def get_FFDP_connection(self):
        if self.flag == 'local':
            parser = configparser.ConfigParser()
            parser.read('/Users/vijaychannappa/Desktop/ydf/atfarm_di/creds.ini')
            username = parser.get('FFDP','username')
            database = parser.get('FFDP','database')
            pwd = parser.get('FFDP','pwd')
            host = parser.get('FFDP','host')
        elif self.flag == 'cloud':
            parser = BaseHook.get_connection('ffdp_redshift')
            username = parser.login
            database = parser.schema
            pwd = parser.password
            host = parser.host
        ffdp_con = psycopg2.connect(user = username,
                                    password = pwd,
                                    host = host,
                                    port = "5439",
                                    database = database)
        # print('engine creating')
        # engine = sa.create_engine(f'redshift+psycopg2://{username}:{pwd}@{host}:5439/{database}')
        # engine = sa.create_engine(f'postgresql://{username}:{pwd}@{host}:5439/{database}')
        # print(engine)
        return ffdp_con

    def is_data_changes_there(self):
        self.source_data_metrics = self.get_table_metrics(type='source')
        self.dest_data_metrics= self.get_table_metrics(type='dest')
        # source_data_metrics.to_csv('sdm.csv',index=False)
        # ffdp_data_metrics.to_csv('fdm.csv',index=False)
        if self.source_data_metrics.equals(self.dest_data_metrics):
            print(f'Source matches with destination, hence no update is needed for {self.dest_table}')
            empty_df = pd.DataFrame()
            return empty_df
        else:
            print(f'Source mismatches with destination, hence update is needed for {self.dest_table}')
            print(f'source metrics for {self.source_tables} is: \n {self.source_data_metrics}')
            print(f'ffdp metrics for {self.dest_table} is: \n {self.dest_data_metrics}')
            return self.dest_data_metrics

    def get_table_metrics(self,type):
        print(self.source_tables)
        print(self.dest_table)
        if type =='dest':
            query = f"""select to_date(max(created_at),'YYYY-MM-DD') as latest_created, to_date(max(updated_at),'YYYY-MM-DD') as latest_updated, count(*) as row_count from {self.dest_schema}.{self.dest_table} where source_id = {self.source_id};"""
            # query = f"""select date_trunc('second',dateadd(minute,-330,max(created))) as latest_created, date_trunc('second',dateadd(minute,-330,max(updated))) as latest_updated, count(*) as row_count from {prefix}_{test_schema}_{db}.{table_name};"""
        elif type == 'source':
            query = f"""select to_date(max(created_at),'YYYY-MM-DD') as latest_created, to_date(max(updated_at),'YYYY-MM-DD') as latest_updated, count(*) as row_count from {self.source_schema}.{self.source_tables};"""
        df = pd.read_sql(query,self.ffdp_con)
        df = df.drop_duplicates() # Dropping duplicates in the legacy db if any
        df['latest_created'] = df['latest_created'].astype(str)
        df['latest_updated'] = df['latest_updated'].astype(str)
        print(df.head())
        return df

    def fetch_source_details(self):
        self.data_metrics = self.is_data_changes_there()
        if self.data_metrics.empty:
            df_to_push = pd.DataFrame()
        else:
            df_to_push = self.get_df_to_push()
            df_to_push = self.fill_dates(df_to_push)
        # df_to_push['source_id'] = source_id
        return df_to_push
    
    def fill_dates(self,df):
        df['created_at'] = df['created_at'].fillna(date.min)
        df['updated_at'] = df['updated_at'].fillna(date.min)
        df['deleted_at'] = df['deleted_at'].fillna(date.min)
        return df

    def get_df_to_push(self):
        if self.data_metrics['row_count'][0] == 0:
            self.query = self.get_query_to_fetch_entire_data()
            df_to_push = self.get_tabular_frame()
            df_to_push = self.unpack_json(df_to_push)
        else:
            self.query = self.get_query_to_fetch_incremental_data(type='source')
            source_df = self.get_tabular_frame()
            source_df = self.unpack_json(source_df)
            self.query = self.get_query_to_fetch_incremental_data(type='destination')
            dest_df = self.get_tabular_frame()
            df_to_push = self.drop_duplicates_entry(source_df,dest_df)
        return df_to_push
    
    def unpack_json(self,df):
        if self.dest_table == 'farmer':
            cols_from_json = ['startedFarming','photoUrl','language']
            for col in cols_from_json:
                self.col = col
                df[col] = df['farm_json'].apply(lambda x: self.convert_json(x))
                df.rename(columns={'photoUrl':'photo_key','startedFarming':'started_farming'},inplace=True)
            col_order = ['user_id','secondary_email','gender','dob','yara_customer','started_farming','partnership','national_id',
            'marital_status','photo_key','language','land_ownership_status','education_level','preferred_comm_channel',
            'social_media_details','cooperative_linkages','source_id','created_at','updated_at','deleted_at']
            # df = df['language'].apply(lambda x: languages.get(alpha2=x.lower()).name if (type(x) == str and len(x) ==2) else x)
            df = df[col_order]
        return df
            
    def drop_duplicates_entry(self,source_df,dest_df):
        merged_df = source_df.append(dest_df)
        incremented_df = merged_df.drop_duplicates(keep=False)
        print(f'incremental df:\n{incremented_df}')
        print(f'number of rows inserted :\n {len(incremented_df)}')
        return incremented_df


    def get_query_to_fetch_incremental_data(self,type):
        self.latest_created = str(datetime.datetime.strptime(self.data_metrics['latest_created'][0],'%Y-%m-%d'))
        self.latest_updated = str(datetime.datetime.strptime(self.data_metrics['latest_updated'][0],'%Y-%m-%d'))
        if type == 'source':
            query = self.get_source_df()
        if type == 'destination':
            query = self.get_dest_df()
        return query

    def get_dest_df(self):
        if self.dest_table == 'users':
            query = f"""SELECT id,
            NULL as auth_id,
            NULL as first_name,
            NULL as last_name,
            NULL as phone, 
            primary_email,
            NULL as country, 
            NULL as address, 
            NULL as zipcode,
            source_id,
            created_at as created_at,
            updated_at as updated_at,
            deleted_at as deleted_at
            from {self.dest_schema}.{self.dest_table}
            where source_id = {self.source_id} and created_at >= '{self.latest_created}' or updated_at >= '{self.latest_updated}';"""
        elif self.dest_table == 'organisations':
            query = f"""SELECT id,name,country,address,{self.source_id} as source_id,created_at,updated_at,NULL as deleted_at
            from {self.dest_schema}.{self.dest_table}
            where source_id = {self.source_id} and created_at >= '{self.latest_created}' or updated_at >= '{self.latest_updated}';"""
        elif self.dest_table == 'farmer':
            query = f"""SELECT user_id,
            secondary_email,  
            gender,
            dob,
            NULL as yara_customer,    
            started_farming,
            NULL as partnership,
            national_id,
            NULL as marital_status,
            photo_key,
            language,
            NULL as land_ownership_status,
            NULL as education_level,
            NULL as preferred_comm_channel,
            NULL as social_media_details,
            NULL as cooperative_linkages,
            source_id,
            created_at as created_at,
            updated_at as updated_at,
            deleted_at as deleted_at
            from {self.dest_schema}.{self.dest_table}
            where source_id = {self.source_id} and created_at >= '{self.latest_created}' or updated_at >= '{self.latest_updated}';"""
        print(query)
        return query
    
    def get_source_df(self):
        if self.dest_table == 'users':
            query = f"""SELECT id,
            NULL as auth_id,
            NULL as first_name,
            NULL as last_name,
            NULL as phone, 
            email as primary_email,
            NULL as country, 
            NULL as address, 
            NULL as zipcode,
            {self.source_id} as source_id,
            created_at as created_at,
            updated_at as updated_at,
            deleted_at as deleted_at
            from {self.source_schema}.{self.source_tables}
            where created_at >= '{self.latest_created}' or updated_at >= '{self.latest_updated}';"""
        elif self.dest_table == 'organisations':
            query = f"""SELECT id,name,country,address,{self.source_id} as source_id,created as created_at,updated as updated_at,NULL as deleted_at
            from {self.source_schema}.{self.source_tables}
             where created_at >= '{self.latest_created}' or updated_at >= '{self.latest_updated}';"""
        elif self.dest_table == 'farmer':
            query = f"""SELECT farmer_id as user_id,
            email as secondary_email,  
            gender as gender,
            date_of_birth as dob,
            NULL as yara_customer,    
            farmer_detail as farm_json,
            NULL as partnership,
            national_id as national_id,
            NULL as marital_status,
            NULL as land_ownership_status,
            NULL as education_level,
            NULL as preferred_comm_channel,
            NULL as social_media_details,
            NULL as cooperative_linkages,
            {self.source_id} as source_id,
            created_at as created_at,
            updated_at as updated_at,
            deleted_at as deleted_at
            from {self.source_schema}.{self.source_tables}
            where created_at >= '{self.latest_created}' or updated_at >= '{self.latest_updated}';"""
        print(query)
        return query


    def get_query_to_fetch_entire_data(self):   
        if self.dest_table == 'users':
            query = f"""SELECT id,
            NULL as auth_id,
            NULL as first_name,
            NULL as last_name,
            NULL as phone, 
            email as primary_email,
            NULL as country, 
            NULL as address, 
            NULL as zipcode,
            {self.source_id} as source_id,
            created_at as created_at,
            updated_at as updated_at,
            deleted_at as deleted_at
            from {self.source_schema}.{self.source_tables};"""
        elif self.dest_table == 'organisations':
            query = f"""SELECT id,name,country,address,{self.source_id} as source_id,created as created_at,updated as updated_at,NULL as deleted_at
            from {self.source_schema}.{self.source_tables};"""
        elif self.dest_table == 'farmer':
            query = f"""SELECT yara_user_id as user_id,
            email as secondary_email,  
            gender as gender,
            date_of_birth as dob,
            NULL as yara_customer,    
            farmer_detail as farm_json,
            NULL as partnership,
            national_id as national_id,
            NULL as marital_status,
            NULL as land_ownership_status,
            NULL as education_level,
            NULL as preferred_comm_channel,
            NULL as social_media_details,
            NULL as cooperative_linkages,
            {self.source_id} as source_id,
            created_at as created_at,
            updated_at as updated_at,
            deleted_at as deleted_at
            from {self.source_schema}.{self.source_tables};"""
        print(query)
        return query
    
    def convert_json(self,x):
        try:
            y = ast.literal_eval(x)
            val = y.get(self.col)
            if self.col == 'startedFarming':
                val = val.strip()
        except Exception as e:
            val = None
        return val
    
    def get_tabular_frame(self):
        df = pd.read_sql(self.query,self.ffdp_con)
        print(f"query results:\n{df.head()}")
        print(f"total number of rows extracted: {len(df)}")
        return df
        
    def push_tabluar_frame_to_s3(self):
        csv_buf = StringIO()
        # out_buf = BytesIO()
        self.df_to_push.to_csv(csv_buf, header=True, index=False)
        # table_frame.to_parquet(out_buf, index=False)
        csv_buf.seek(0)
        # bucket = 'ffdp-data-general-stage'
        self.client.put_object(Bucket=self.bucket, Body=csv_buf.getvalue(), Key=f'farm_field_profile/{self.source_tables}_to_{self.dest_table}_{str(datetime.date.today())}.csv')
        print(f'pushed {self.source_tables} data to S3')


    def copy_from_s3_to_ffdp(self):
            dt = str(datetime.date.today() - datetime.timedelta(days=0))
            # for file in self.my_bucket.objects.all():
            
            for file in self.my_bucket.objects.filter(Prefix=f"{self.folder_path}/"):
                file_key = file.key
                file_name = file_key.split('/')[-1]
                if file_name.split('_to_')[0] == self.source_tables and file_name.split('_to_')[-1].rsplit('_',1)[0] == self.dest_table and dt in file_name:
                    self.s3_url = f's3://{self.bucket}/{file_key}'
                    query = self.get_formatted_copy_query()
                    cursor = self.ffdp_con.cursor()
                    try:
                        cursor.execute(query)
                        print('query executed successfully')
                        cursor.close()
                    except Exception as e:
                        cursor.execute("ROLLBACK")
                        print(f"Query could not be executed due to {e} \n and hence {self.dest_table} not updated/loaded")
                        cursor.close()
                else:
                    continue
        #cursor.close()



    def clear_bucket(self):
        prefix = self.folder_path
        response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        file_count = response.get('KeyCount')
        if file_count > 0:
            print(f"Found {file_count} files inside the {prefix} folder")
            for object in response['Contents']:
                print(f"Deleting {object['Key']}")
                self.client.delete_object(Bucket=self.bucket, Key=object['Key'])
        else:
            print(f"Found {file_count} files inside the {prefix} folder")

    def get_formatted_copy_query(self):
        cred = boto3.Session().get_credentials()
        if self.flag == 'local':
            query = f"""COPY {self.dest_schema}.{self.dest_table} FROM '{self.s3_url}' FORMAT as CSV dateformat 'auto' timeformat 'auto' IGNOREHEADER 1 credentials 'aws_access_key_id={cred.access_key};aws_secret_access_key={cred.secret_key};token={cred.token}';"""
        if self.flag =='cloud':
            #query = f"""COPY at_{test_schema}_{db}.{table} FROM '{s3_url}' FORMAT as CSV dateformat 'auto' timeformat 'auto' IGNOREHEADER 1 iam_role 'arn:aws:iam::504749939156:role/ffdp-stage';"""
            query = f"""COPY {self.dest_schema}.{self.dest_table} FROM '{self.s3_url}' FORMAT as CSV dateformat 'auto' timeformat 'auto' IGNOREHEADER 1 iam_role 'arn:aws:iam::504749939156:role/ffdp-farm-field-stage';"""
        print(f"Pushing data into {self.dest_schema}.{self.dest_table} from {self.s3_url}")
        print(f"Query for pushing:\n {query}")
        return query



if __name__ == "__main__":
    start = time.time()
    source_id = 2
    flag = 'local'
    truncate = False

    # load_org_farm_profile = push_data_to_farm_profile(source_schema='at_public_core',
    #                                                    dest_schema ='farm_field_profile',
    #                                                    source_tables = 'm_organisations',
    #                                                    dest_table = 'organisations',
    #                                                    source_id = 2,
    #                                                    flag = 'local',
    #                                                    bucket = 'ffdp-data-general-stage')

    # load_org_farm_profile.fetch_push_data()
    load_user_farm_profile = push_data_to_farm_profile(source_schema='fc_public_auth_service',
                                                       dest_schema ='farm_field_profile',
                                                       source_tables = 'user',
                                                       dest_table = 'users',
                                                       source_id = 2,
                                                       flag = 'local',
                                                       bucket = 'ffdp-data-general-stage',
                                                       truncate=truncate)
    load_user_farm_profile.fetch_push_data()

    load_farmer_farm_profile = push_data_to_farm_profile(source_schema='fc_public_farmer_service',
                                                       dest_schema ='farm_field_profile',
                                                       source_tables = 'farmer',
                                                       dest_table = 'farmer',
                                                       source_id = 2,
                                                       flag = 'local',
                                                       bucket = 'ffdp-data-general-stage',
                                                       truncate=truncate)
    load_farmer_farm_profile.fetch_push_data()

    end = time.time()
    print(f'Final Time is {end-start}')