# -*- coding: utf-8 -*-

# Download the source files
# -http hook

# Stage to s3
# -aws hook

# Load to redshift
# -postgreshook , aws_hook

# Data quality check
# -postgreshook
import sys
sys.path.append('/Users/zwin/airflow_home/plugins')
sys.path.append('/Users/zwin/airflow_home/dags')
sys.path.append('/Users/zwin/airflow_home/logs')

from datetime import datetime
from airflow import DAG
from operators import DownloadFromKaggleOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.contrib.hooks.redshift_hook import RedshiftHook
import pandas as pd
import logging

#variables
s3bucket = Variable.get('s3_bucket')

def process_files(**kwargs):
    try:
        if (kwargs['input_file'] == 'HospInfo.csv'):
            df = pd.read_csv(kwargs['input_file'] ,dtype={'ZIP Code':str})
            df1 = df[['Provider ID', 'Hospital Name', 'Address', 'City', 'State', 'ZIP Code',
           'County Name', 'Phone Number', 'Hospital Type', 'Hospital Ownership',
           'Emergency Services', 'Meets criteria for meaningful use of EHRs']].drop_duplicates()
            df1.fillna(None,inplace=True)
            
            df2 = df[['Provider ID','Hospital overall rating', 'Hospital overall rating footnote',
           'Mortality national comparison',
           'Mortality national comparison footnote',
           'Safety of care national comparison',
           'Safety of care national comparison footnote',
           'Readmission national comparison',
           'Readmission national comparison footnote',
           'Patient experience national comparison',
           'Patient experience national comparison footnote',
           'Effectiveness of care national comparison',
           'Effectiveness of care national comparison footnote',
           'Timeliness of care national comparison',
           'Timeliness of care national comparison footnote',
           'Efficient use of medical imaging national comparison',
           'Efficient use of medical imaging national comparison footnote']].drop_duplicates()
            
            df1.to_csv('hospital_general_info.csv', sep='|',index=False)
            df2.to_csv('hospital_ratings.csv',sep='|',index=False)
            
            
        elif(kwargs['input_file'] =='database-of-hpsa-and-low-income-zip-codes-for-issuers-subject-to-the-alternate-ecp-standard-for-the-purposes-of-qhp-certification.csv'):
            df = pd.read_csv(kwargs['input_file'] ,dtype={'ZIP':str})
            df1 = df[['ZIP', 'City/Location Name', 'STATE','County Name', 'Zip Code Designation']].drop_duplicates()
            df1.to_csv('low_income_zip.csv', sep='|',index=False)
            
        elif (kwargs['input_file']=='postcode_level_averages.csv'):
            df = pd.read_csv(kwargs['input_file'], dtype={'zipcode':str})
            df = df[df['zipcode']!= 0]
            df1 = df[['zipcode','total_pop','total_income','avg_income']].drop_duplicates()
            df1.to_csv('avg_income_per_zipcode.csv', sep='|', index=False)
            
            
    except Exception as e:
        logging.info(e)
        return False
    return True

def s3upload(**kwargs):
    try:
        hook = S3Hook(aws_conn_id = 'aws_credentials')
        bucket = s3bucket
        if (hook.check_for_bucket(bucket)):
            hook.load_file(kwargs['filename'], bucket_name = bucket, key=kwargs['filename'],replace=True)
    except Exception as e:
        logging.info(e)
    

default_args ={
    'owner':'Harry',
    'start_date':datetime.now(),
    'email':'zawnaingwynn@gmail.com',
    'email_on_failure':False,
    'schedule_interval':'0 2 * * *'
    }

dag = DAG(
    'low_income_hospitals',
    default_args = default_args,
    description = 'Load and transform low-income area hospitals from Kaggle to Redshift with Airflow'
    )

start_operator = DummyOperator(task_id = 'Begin_execution', dag= dag)

download_hospinfo = DownloadFromKaggleOperator(
    task_id = 'Download_Hospital_Dataset',
    dag = dag,
    dataset = 'cms/hospital-general-information'
    )

download_low_income_zip = DownloadFromKaggleOperator(
    task_id = 'Download_Low_Income_Zipcode_Dataset',
    dag = dag,
    dataset = 'cms/cms-hpsa-low-income-zip-code-database'
    )

download_average_income = DownloadFromKaggleOperator(
    task_id = 'Download_Average_Income_per_Zipcode_Dataset',
    dag = dag,
    dataset = 'hamishgunasekara/average-income-per-zip-code-usa-2018'
    )

process_hospital_data = PythonOperator(
    task_id = 'Process_Hospital_Dataset',
    dag = dag,
    provide_context = True,
    python_callable = process_files,
    op_kwargs={'input_file':'HospInfo.csv'}
    )

process_low_income_data = PythonOperator(
    task_id = 'Process_Low_Income_Zipcode_Dataset',
    dag = dag,
    provide_context = True,
    python_callable = process_files,
    op_kwargs={'input_file':'database-of-hpsa-and-low-income-zip-codes-for-issuers-subject-to-the-alternate-ecp-standard-for-the-purposes-of-qhp-certification.csv'}
    )

process_avg_income_data = PythonOperator(
    task_id = 'Process_Avg_Income_per_Zipcode_Dataset',
    dag = dag,
    provide_context = True,
    python_callable = process_files,
    op_kwargs={'input_file':'postcode_level_averages.csv'}
    )

s3upload_hospital_data = PythonOperator(
    task_id = 'Stage_Hospital_Info_to_s3',
    dag = dag,
    provide_context = True,
    python_callable = s3upload,
    op_kwargs = {'filename':'hospital_general_info.csv'}
    )

s3upload_hospital_rating_data = PythonOperator(
    task_id = 'Stage_Hospital_Rating_to_s3',
    dag = dag,
    provide_context = True,
    python_callable = s3upload,
    op_kwargs = {'filename':'hospital_ratings.csv'}
    )

s3upload_low_income_data = PythonOperator(
    task_id = 'Stage_Low_Income_Zipcodes_to_s3',
    dag = dag,
    provide_context = True,
    python_callable = s3upload,
    op_kwargs = {'filename':'low_income_zip.csv'}
    )

s3upload_avg_income_zip_data = PythonOperator(
    task_id = 'Stage_Each_Zip_Avg_Income_to_s3',
    dag = dag,
    provide_context = True,
    python_callable = s3upload,
    op_kwargs = {'filename':'avg_income_per_zipcode.csv'}
    )

sql = open('create_tables.sql','r').read()
create_tables = PostgresOperator(
    task_id = 'Create_Tables_in_Redshift',
    dag = dag,
    postgres_conn_id = 'redshift',
    sql = sql
    )


load_hospital_locations = S3ToRedshiftOperator(
    task_id = 'Load_hopsital_locations',
    dag = dag,
    schema = 'PUBLIC',
    table = 'hospital_locations',
    s3_bucket = s3bucket,
    s3_key = 'hospital_general_info.csv',
    aws_conn_id = 'aws_credentials',
    redshift_conn_id = 'redshift',
    autocommit = True,
    copy_options=['CSV delimiter \'|\' IGNOREHEADER 1'],
    truncate_table=True
    )

load_hospital_ratings = S3ToRedshiftOperator(
    task_id = 'Load_hopsital_ratings',
    dag = dag,
    schema = 'PUBLIC',
    table = 'hospital_ratings',
    s3_bucket = s3bucket,
    s3_key = 'hospital_ratings.csv',
    aws_conn_id = 'aws_credentials',
    redshift_conn_id = 'redshift',
    autocommit = True,
    copy_options = ['CSV delimiter \'|\' IGNOREHEADER 1'],
    truncate_table=True
    )

load_low_income = S3ToRedshiftOperator(
    task_id = 'Load_low_income_areas',
    dag = dag,
    schema = 'PUBLIC',
    table = 'low_income_area',
    s3_bucket = s3bucket,
    s3_key = 'low_income_zip.csv',
    aws_conn_id = 'aws_credentials',
    redshift_conn_id = 'redshift',
    autocommit = True,
    copy_options = ['CSV delimiter \'|\' IGNOREHEADER 1'],
    truncate_table=True
    )

load_avg_income = S3ToRedshiftOperator(
    task_id = 'Load_income_for_each_zip',
    dag = dag,
    schema = 'PUBLIC',
    table = 'income_per_zip',
    s3_bucket = s3bucket,
    s3_key = 'avg_income_per_zipcode.csv',
    aws_conn_id = 'aws_credentials',
    redshift_conn_id = 'redshift',
    autocommit = True,
    copy_options= ['CSV delimiter \'|\' IGNOREHEADER 1'],
    truncate_table=True
    )


end_operator = DummyOperator(task_id = 'End_execution', dag = dag)


start_operator>>download_hospinfo>>process_hospital_data>>s3upload_hospital_data>>create_tables>>load_hospital_locations>>end_operator
start_operator>>download_hospinfo>>process_hospital_data>>s3upload_hospital_rating_data>>create_tables>>load_hospital_ratings>>end_operator
start_operator>>download_low_income_zip>>process_low_income_data>>s3upload_low_income_data>>create_tables>>load_low_income>>end_operator
start_operator>>download_average_income>>process_avg_income_data>>s3upload_avg_income_zip_data>>create_tables>>load_avg_income>>end_operator

#start_operator>>create_tables>>load_hospital_locations>>end_operator
#start_operator>>create_tables>>load_hospital_ratings>>end_operator
#start_operator>>create_tables>>load_low_income>>end_operator
#start_operator>>create_tables>>load_avg_income>>end_operator
