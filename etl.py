import boto3
from botocore import UNSIGNED
from botocore.client import Config
import pandas as pd
from pathlib import Path
import os, glob
from util import get_d2b_assessment_conn
from datetime import datetime





def list_s3_objects():
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

    bucket_name = "tenalytics-internship-bucket"
    prefix = "orders_data"

    # List objects in the s3 bucket
    response = s3.list_objects(Bucket=bucket_name, Prefix=prefix)
    return s3, response, bucket_name


def extract(s3, response, bucket_name):
    
    # Check if objects are found
    if 'Contents' in response:
        for obj in response['Contents']:
            # Print boject
            print(obj['Key'])
    else:
        print("No objects found in the specified bucket and prefix.")

    # Check if objects are found
    if 'Contents' in response:
        for obj in response['Contents']:
            folder = 'orders_data'
            file_name = obj['Key'].split('/')[-1]
            path = Path(folder) / file_name
            if file_name == '': continue
            # Download the file
            s3.download_file(bucket_name, obj['Key'], path)
            print(f"Successfully downloaded {file_name}")
    else:
        print('No object found in the specified bucket and prefix.')

def transform():
    files = glob.glob('./orders_data/*')
    for file in files:
        file_name = file.split('/')[2][:-4]
        folder = 'transformed_data'
        path = Path(folder) / file_name
        df = pd.read_csv(file)
        df['ingestion_date'] = pd.Timestamp('now').strftime('%Y-%m-%d')
        print(df.head())
        df.to_csv(path, index=False)


def load_to_db():
    files = glob.glob('./transformed_data/*')
    engine = get_d2b_assessment_conn()
    for file in files:
        file_name = file.split('/')[2]
        df = pd.read_csv(file)
        df['ingestion_date'] = pd.to_datetime(datetime.now()).strftime('%Y-%m-%d')
        df.info()
        table_exist_quey = f"""
        select exists (select 1 from information_schema.tables where table_name = '{file_name}')
        """

        table_exist = pd.read_sql(table_exist_quey, con=engine)
        if table_exist['exists'][0] == False:
            df.to_sql(file_name, con=engine, schema='solochik6145_staging', index=False)
            print(f'Successfully created table {file_name} in the database')
        else:
            query = f'''
            select max(ingestion_date) from solochik6145_staging.{file_name}
            '''
            last_updated = pd.read_sql(query, con=engine).values[0][0]
            last_updated = pd.to_datetime(last_updated).strftime('%Y-%m-%d') # Convert to datetime.date object
            
            new_data = df[df['ingestion_date'] > last_updated]
            new_data.to_sql(file_name, con=engine, schema='solochik6145_staging', index=False, if_exists='append')
            print(f'Successfully written {new_data.shape[0]} rows to {file_name} table')
      

def agg_public_holiday():
    engine = get_d2b_assessment_conn()
    query = '''
    select count(*) from solochik6145_staging.orders s join if_common.dim_dates d on s.order_date = d.calendar_dt 
    '''
    df = pd.read_sql(query, con=engine)
    print(df)




def parent_etl():
    # schema = os.getenv("STAGING_SCHEMA")
    # s3, response, bucket_name = list_s3_objects()
    # extract(s3, response, bucket_name)
    # transform()
    load_to_db()
    #agg_public_holiday()

if __name__ == '__main__':
    parent_etl()
        
