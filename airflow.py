from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSFileTransformOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
    BigQueryCheckOperator
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging
# Configuration
PROJECT_ID = 'gen-lang-client-0351304067'
DATASET_ID = 'your_etl_dataset'  # Don't include project ID here
STG_TABLE_ID = 'stg_table'
FINAL_TABLE_ID = 'final_table'
GCS_BUCKET = 'us-central1-dev-b99eb0a8-bucket'
GCS_PREFIX = 'data/input/'
GCS_ARCHIVE_PREFIX = 'data/archive/'
FILE_NAME = 'Cleaned-COVID.csv'

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),  # Use recent date
    'email': ['jigarcars.harsh@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'gcs_to_bigquery_etl',
    default_args=default_args,
    description='ETL pipeline from GCS to BigQuery with transformations',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    tags=['etl', 'bigquery', 'gcs'],
)

# Task1: File sensor task
file_sensor = GCSObjectExistenceSensor(
    task_id='check_gcs_file',
    bucket=GCS_BUCKET,
    object=f'{GCS_PREFIX}{FILE_NAME}',
    timeout=300,
    poke_interval=60,
    dag=dag,
)

# Task2: Create Staging table in the big query dataset which you created manually
create_stg_table = BigQueryCreateEmptyTableOperator(
    task_id='create_staging_table',
    dataset_id=DATASET_ID,
    table_id=STG_TABLE_ID,
    project_id=PROJECT_ID,
    schema_fields=[
        {'name': 'entity', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Day', 'type': 'DATETIME', 'mode': 'NULLABLE'},
        {'name': 'total_confirmed_deaths', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    ],
    exists_ok=True,
    dag=dag,
)

# Task3: Create a Final table 
create_final_table = BigQueryCreateEmptyTableOperator(
    task_id='create_final_table',
    dataset_id=DATASET_ID,
    table_id=FINAL_TABLE_ID,
    project_id=PROJECT_ID,
    schema_fields=[
        {'name': 'entity', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Day', 'type': 'DATETIME', 'mode': 'NULLABLE'},
        {'name': 'total_confirmed_deaths', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    ],
    exists_ok=True,
    dag=dag,
)

# Task4: After the table gets created load the data from csv to the bigquery staging table
load_to_staging = GCSToBigQueryOperator(
    task_id='load_gcs_to_staging',
    bucket=GCS_BUCKET,
    source_objects=[f'{GCS_PREFIX}{FILE_NAME}'],
    destination_project_dataset_table=f'{PROJECT_ID}.{DATASET_ID}.{STG_TABLE_ID}',
    schema_fields=None,  # Use autodetect or predefined schema
    autodetect=True,
    source_format='CSV',
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',  # Replace staging table data
    create_disposition='CREATE_IF_NEEDED',
    allow_quoted_newlines=True,
    allow_jagged_rows=False,
    dag=dag,
)

#Task5 : Transform and load data from staging to final table 
transform_and_load = BigQueryInsertJobOperator(
    task_id='transform_staging_to_final',
    configuration={
        "query": {
            "query": f"""
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.{FINAL_TABLE_ID}` 
            SELECT 
                entity,
                Day,
                total_confirmed_deaths
            FROM `{PROJECT_ID}.{DATASET_ID}.{STG_TABLE_ID}`
            WHERE total_confirmed_deaths > 0
            """,
            "useLegacySql": False,
            "priority": "INTERACTIVE",
        }
    },
    dag=dag,
)
