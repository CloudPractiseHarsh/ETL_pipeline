import json
import csv
import io
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from google.cloud import storage
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryCheckOperator
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
# from airflow.providers.google.cloud.sensors.dataflow import DataflowJobStatus
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator, BeamRunnerType
# from airflow.providers.google.cloud.operators.dataflow import DataflowJobStatus
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Try to import EmptyOperator, fallback to DummyOperator
try:
    from airflow.operators.empty import EmptyOperator
except ImportError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator

PROJECT_ID = 'gen-lang-client-0351304067'
REGION = 'us-central1'
GCS_BUCKET = 'us-central1-dev-c626561e-bucket'
GCS_INPUT_PATH = 'data/input/Cleaned-COVID.csv'
DATAFLOW_JOB_NAME = 'etl-pipeline'
BQ_DATASET = 'processed_data'
BQ_TABLE = 'transformed_table'
DATAFLOW_PYTHON_FILE = 'gs://{GCS_BUCKET}/dataflow_transform.py'
# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

# Initialize DAG
dag = DAG(
    'gcs_dataflow_bigquery_official',
    default_args=default_args,
    description='ETL Pipeline: GCS â†’ Dataflow â†’ BigQuery (Official Documentation)',
    schedule_interval='@daily',
    max_active_runs=1,
    tags=['etl', 'gcs', 'dataflow', 'bigquery', 'official'],
)


# Updated validation functions for your DAG
def check_input_files(**context):
    """
    Enhanced input file validation with comprehensive checks
    """
    try:
        # Get file list from previous task
        files = context['task_instance'].xcom_pull(task_ids='list_input_files')
        
        if not files:
            raise AirflowException("No input files found in GCS")
        
        logging.info(f"Starting validation of {len(files)} files")
        
        # Initialize validation counters
        validation_summary = {
            'total_files': len(files),
            'valid_files': 0,
            'invalid_files': 0,
            'total_records': 0,
            'total_size_mb': 0,
            'validation_errors': [],
            'validation_warnings': []
        }
        
        # Validate each file
        for file_path in files:
            try:
                file_result = validate_single_file(file_path, context)
                
                # Update counters
                validation_summary['valid_files'] += 1
                validation_summary['total_records'] += file_result.get('record_count', 0)
                validation_summary['total_size_mb'] += file_result.get('size_mb', 0)
                
                logging.info(f"âœ“ {file_path}: {file_result.get('record_count', 0)} records, "
                           f"{file_result.get('size_mb', 0):.2f} MB")
                
            except Exception as e:
                validation_summary['invalid_files'] += 1
                error_msg = f"âŒ {file_path}: {str(e)}"
                validation_summary['validation_errors'].append(error_msg)
                logging.error(error_msg)
        
        # Store results for downstream tasks
        context['task_instance'].xcom_push(key='validation_summary', value=validation_summary)
        
        # Log summary
        logging.info(f"""
        ðŸ“Š VALIDATION SUMMARY:
        âœ… Valid files: {validation_summary['valid_files']}
        âŒ Invalid files: {validation_summary['invalid_files']}
        ðŸ“„ Total records: {validation_summary['total_records']:,}
        ðŸ’¾ Total size: {validation_summary['total_size_mb']:.2f} MB
        """)
        
        # Fail if any files are invalid
        if validation_summary['invalid_files'] > 0:
            error_message = f"Validation failed: {validation_summary['invalid_files']} invalid files"
            logging.error(error_message)
            for error in validation_summary['validation_errors']:
                logging.error(f"  {error}")
            raise AirflowException(error_message)
        
        # Warn if no data found
        if validation_summary['total_records'] == 0:
            raise AirflowException("No data records found in any files")
        
        logging.info("ðŸŽ‰ All files passed validation!")
        return True
        
    except Exception as e:
        logging.error(f"File validation failed: {str(e)}")
        raise AirflowException(f"File validation error: {str(e)}")

def validate_single_file(file_path: str, context) -> Dict[str, Any]:
    """
    Validate individual file with detailed checks
    """
    # Initialize GCS client
    client = storage.Client()
    
    # Handle both full GCS paths and relative paths
    if file_path.startswith('gs://'):
        # Full GCS path: gs://bucket/path/file.json
        path_parts = file_path.replace('gs://', '').split('/', 1)
        bucket_name = path_parts[0]
        object_name = path_parts[1]
    else:
        # Relative path: just the filename or folder/filename
        bucket_name = GCS_BUCKET
        object_name = file_path
    
    # Get file metadata
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    
    # Check if file exists
    if not blob.exists():
        raise ValueError(f"File not found in GCS: gs://{bucket_name}/{object_name}")
    
    # Get file properties
    blob.reload()  # Refresh metadata
    file_size_bytes = blob.size
    file_size_mb = file_size_bytes / (1024 * 1024)
    file_created = blob.time_created
    file_updated = blob.updated
    
    # Validate file size
    if file_size_bytes == 0:
        raise ValueError("File is empty (0 bytes)")
    
    # Warn about large files (>100MB)
    if file_size_mb > 100:
        logging.warning(f"Large file detected: {file_size_mb:.2f} MB")
    
    # Check file age (warn if older than 7 days)
    file_age = datetime.now(file_created.tzinfo) - file_created
    if file_age.days > 7:
        logging.warning(f"Old file: {file_age.days} days old")
    
    # Download and validate content
    try:
        content = blob.download_as_text(encoding='utf-8')
    except UnicodeDecodeError:
        raise ValueError("File encoding error - not valid UTF-8")
    
    # Validate based on file extension
    filename = object_name.split('/')[-1].lower()
    
    if filename.endswith('.json'):
        record_count = validate_json_content(content, filename)
    elif filename.endswith('.csv'):
        record_count = validate_csv_content(content, filename)
    elif filename.endswith(('.jsonl', '.ndjson')):
        record_count = validate_jsonl_content(content, filename)
    else:
        # Default validation for other text files
        record_count = validate_text_content(content, filename)
    
    return {
        'filename': filename,
        'record_count': record_count,
        'size_mb': file_size_mb,
        'created': file_created.isoformat(),
        'validation_status': 'valid'
    }

def validate_json_content(content: str, filename: str) -> int:
    """Validate JSON file structure and content"""
    try:
        data = json.loads(content)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON syntax: {str(e)}")
    
    if isinstance(data, list):
        # Array of records
        record_count = len(data)
        if record_count == 0:
            raise ValueError("JSON array is empty")
        
        # Validate sample records
        sample_size = min(5, record_count)
        for i, record in enumerate(data[:sample_size]):
            validate_record_structure(record, f"record {i+1}")
            
    elif isinstance(data, dict):
        # Single record
        record_count = 1
        validate_record_structure(data, "single record")
    else:
        raise ValueError("JSON must contain object(s), not primitive values")
    
    return record_count

def validate_csv_content(content: str, filename: str) -> int:
    """Validate CSV file structure and content"""
    try:
        # Parse CSV with DictReader
        reader = csv.DictReader(io.StringIO(content))
        headers = reader.fieldnames
        
        if not headers:
            raise ValueError("CSV has no header row")
        
        # Check for required columns
        required_cols = ['entity', 'Day', 'total_confirmed_deaths']  # Adjust based on your schema
        missing_cols = [col for col in required_cols if col not in headers]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        # Read and validate records
        records = list(reader)
        record_count = len(records)
        
        if record_count == 0:
            raise ValueError("CSV has no data rows")
        
        # Validate sample records
        sample_size = min(5, record_count)
        for i, record in enumerate(records[:sample_size]):
            validate_csv_record(record, f"row {i+2}")  # +2 because of header row
        
        return record_count
        
    except csv.Error as e:
        raise ValueError(f"CSV parsing error: {str(e)}")

def validate_jsonl_content(content: str, filename: str) -> int:
    """Validate JSON Lines file"""
    lines = [line.strip() for line in content.split('\n') if line.strip()]
    
    if not lines:
        raise ValueError("JSONL file is empty")
    
    record_count = 0
    for line_num, line in enumerate(lines, 1):
        try:
            record = json.loads(line)
            validate_record_structure(record, f"line {line_num}")
            record_count += 1
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON on line {line_num}: {str(e)}")
    
    return record_count

def validate_text_content(content: str, filename: str) -> int:
    """Basic text file validation"""
    lines = [line.strip() for line in content.split('\n') if line.strip()]
    
    if not lines:
        raise ValueError("Text file is empty")
    
    return len(lines)

def validate_record_structure(record: Dict[str, Any], context: str):
    """Validate individual record structure"""
    if not isinstance(record, dict):
        raise ValueError(f"{context}: Must be a JSON object")
    
    # Check required fields
    required_fields = ['id', 'name', 'value']  # Adjust for your schema
    for field in required_fields:
        if field not in record:
            raise ValueError(f"{context}: Missing required field '{field}'")
        
        if not record[field] and record[field] != 0:  # Allow 0 but not None/empty
            raise ValueError(f"{context}: Field '{field}' is empty")
    
    # Validate field types
    if not isinstance(record['id'], (str, int)):
        raise ValueError(f"{context}: 'id' must be string or number")
    
    if not isinstance(record['name'], str):
        raise ValueError(f"{context}: 'name' must be string")
    
    if 'value' in record:
        try:
            float(record['value'])
        except (ValueError, TypeError):
            raise ValueError(f"{context}: 'value' must be numeric")
    
    # Validate timestamp if present
    if 'timestamp' in record and record['timestamp']:
        validate_timestamp(record['timestamp'], context)

# def validate_csv_record(record: Dict[str, str], context: str):
#     """Validate CSV record (all values are strings from CSV)"""
#     # Check for required fields
#     if not record.get('entity', '').strip():
#         raise ValueError(f"{context}: 'id' field is empty")
    
#     if not record.get('Day', '').strip():
#         raise ValueError(f"{context}: 'name' field is empty")
    
#     # Validate numeric fields
#     if 'value' in record and record['value']:
#         try:
#             float(record['value'])
#         except ValueError:
#             raise ValueError(f"{context}: 'value' is not numeric: '{record['value']}'")
def validate_csv_record(record: Dict[str, str], context: str):
    """Validate CSV record for COVID data"""
    required_fields = ['entity', 'Day', 'total_confirmed_deaths']
    for field in required_fields:
        if not record.get(field, '').strip():
            logging.warning(f"{context}: '{field}' field is empty")
    
    if 'total_confirmed_deaths' in record and record['total_confirmed_deaths']:
        try:
            float(record['total_confirmed_deaths'])
        except ValueError:
            raise ValueError(f"{context}: 'total_confirmed_deaths' is not numeric: '{record['total_confirmed_deaths']}'")
    
    if 'Day' in record and record['Day']:
        try:
            datetime.strptime(record['Day'], '%Y-%m-%d')
        except ValueError:
            raise ValueError(f"{context}: 'Day' is not in valid date format (YYYY-MM-DD): '{record['Day']}'")

def validate_timestamp(timestamp_val: Any, context: str):
    """Validate timestamp format"""
    if not timestamp_val:
        return
    
    # Common timestamp formats
    formats = [
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%dT%H:%M:%S.%f',
        '%Y-%m-%dT%H:%M:%SZ',
        '%Y-%m-%d'
    ]
    
    timestamp_str = str(timestamp_val)
    
    for fmt in formats:
        try:
            datetime.strptime(timestamp_str, fmt)
            return  # Valid format
        except ValueError:
            continue
    
    raise ValueError(f"{context}: Invalid timestamp format '{timestamp_val}'")

# Enhanced prepare_dataflow_parameters function
def prepare_dataflow_parameters(**context):
    """
    Prepare parameters with validation results
    """
    execution_date = context['execution_date']
    
    # Get validation summary
    validation_summary = context['task_instance'].xcom_pull(
        task_ids='validate_input_files', 
        key='validation_summary'
    )
    
    # Get actual file list
    files = context['task_instance'].xcom_pull(task_ids='list_input_files')
    
    # Build parameters
    parameters = {
        'input_path': f"gs://{GCS_BUCKET}/{GCS_INPUT_PATH}*.json",  # Pattern for Dataflow
        'output_table': f"{PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE}",
        'execution_date': execution_date.strftime('%Y-%m-%d'),
        'temp_location': f"gs://{GCS_BUCKET}/temp/",
        'staging_location': f"gs://{GCS_BUCKET}/staging/",
        # Add validation metadata
        'expected_record_count': validation_summary.get('total_records', 0),
        'input_file_count': validation_summary.get('valid_files', 0),
        'total_size_mb': validation_summary.get('total_size_mb', 0)
    }
    
    logging.info(f"Prepared Dataflow parameters: {parameters}", files, validation_summary, execution_date)
    return parameters
# Task 1: Check for input files in GCS
check_input_sensor = GCSObjectExistenceSensor(
    task_id='check_input_files_exist',
    bucket=GCS_BUCKET,
    object=f'{GCS_INPUT_PATH}',  # Ensure this is a specific file, not a directory
    google_cloud_conn_id='google_cloud_default',  # Correct parameter
    timeout=300,
    poke_interval=60,
    dag=dag,
)

# Task 2: List and validate input files
list_input_files = GCSListObjectsOperator(
    task_id='list_input_files',
    bucket=GCS_BUCKET,
    prefix=GCS_INPUT_PATH,
    gcp_conn_id='google_cloud_default',
    dag=dag,
)

# Task 3: Validate input files
validate_files = PythonOperator(
    task_id='validate_input_files',
    python_callable=check_input_files,
    dag=dag,
)

# Task 4: Prepare Dataflow parameters
prepare_params = PythonOperator(
    task_id='prepare_dataflow_parameters',
    python_callable=prepare_dataflow_parameters,
    dag=dag,
)

# Task 5: Create BigQuery dataset if not exists
create_bq_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_bigquery_dataset',
    dataset_id=BQ_DATASET,
    project_id=PROJECT_ID,
    location=REGION,
    exists_ok=True,
    gcp_conn_id='google_cloud_default',
    dag=dag,
)


# Updated Beam operator configuration
run_dataflow_pipeline = BeamRunPythonPipelineOperator(
    task_id='run_dataflow_pipeline',
    runner=BeamRunnerType.DataflowRunner,
    py_file=f'gs://{GCS_BUCKET}/dataflow_transform.py',
    pipeline_options={
        'project': PROJECT_ID,
        'region': REGION,
        'job_name': f'{DATAFLOW_JOB_NAME}-{{{{ ds_nodash }}}}',  # Use template for unique job name
        'temp_location': f'gs://{GCS_BUCKET}/temp/',
        'staging_location': f'gs://{GCS_BUCKET}/staging/',
        'input_file': f'gs://{GCS_BUCKET}/{GCS_INPUT_PATH}',
        'output_table': f'{PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE}',
        'runner': 'DataflowRunner',
        'save_main_session': True,
        'setup_file': None  # Add if you have a setup.py file
    },

    gcp_conn_id='google_cloud_default',
    py_interpreter='python3',
    dag=dag,
)

check_input_sensor >> list_input_files >> validate_files >> prepare_params >> create_bq_dataset >> run_dataflow_pipeline
