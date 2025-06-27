import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import logging
from datetime import datetime
import csv
import io
import argparse
import os

# Set up logging
logging.getLogger().setLevel(logging.INFO)

def parse_csv_line(line):
    """Parse a single CSV line into a dictionary."""
    try:
        # Create a StringIO object from the line
        csv_file = io.StringIO(line)
        
        # We need to provide the headers since we're processing line by line
        # Adjust these headers to match your actual CSV structure
        headers = ['entity', 'Day', 'total_confirmed_deaths']  # Add other columns as needed
        
        reader = csv.DictReader(csv_file, fieldnames=headers)
        row = next(reader)
        
        return row
    except Exception as e:
        logging.error(f"Error parsing CSV line '{line}': {str(e)}")
        return None

def transform_row(element):
    """Apply transformations to a CSV row."""
    try:
        if not element:
            return None
            
        # Ensure required fields are present and valid
        if not element.get('entity') or not element.get('Day') or not element.get('total_confirmed_deaths'):
            logging.warning(f"Skipping row with missing fields: {element}")
            return None
        
        # Convert total_confirmed_deaths to integer
        try:
            total_confirmed_deaths = int(float(element['total_confirmed_deaths']))
        except (ValueError, TypeError):
            logging.warning(f"Invalid total_confirmed_deaths in row: {element}")
            return None
        
        # Validate and standardize Day to YYYY-MM-DD
        try:
            # Handle different possible date formats
            day_str = element['Day'].strip()
            if len(day_str) == 10 and day_str.count('-') == 2:
                day = datetime.strptime(day_str, '%Y-%m-%d').strftime('%Y-%m-%d')
            else:
                logging.warning(f"Invalid date format in row: {element}")
                return None
        except (ValueError, AttributeError):
            logging.warning(f"Invalid date format in row: {element}")
            return None
        
        # Create transformed row
        transformed = {
            'entity': str(element['entity']).strip(),
            'Day': day,
            'total_confirmed_deaths': total_confirmed_deaths,
            'processed_timestamp': datetime.utcnow().isoformat() + 'Z'  # Add Z for UTC
        }
        return transformed
    except Exception as e:
        logging.error(f"Error transforming row {element}: {str(e)}")
        return None

class CountRecords(beam.DoFn):
    """Count records and create audit log entry."""
    
    def __init__(self, input_file):
        self.input_file = input_file
        self.record_count = 0
        
    def process(self, element):
        self.record_count += 1
        yield element
        
    def finish_bundle(self):
        # Log the count periodically
        if self.record_count % 1000 == 0:
            logging.info(f"Processed {self.record_count} records so far")

def create_audit_log(input_file, record_count):
    """Create audit log entry."""
    file_name = os.path.basename(input_file)
    ingestion_time = datetime.utcnow().isoformat() + 'Z'
    
    audit_entry = {
        'file_name': file_name,
        'ingestion_time': ingestion_time,
        'record_count': record_count,
        'input_path': input_file,
        'pipeline_status': 'completed',
        'processed_timestamp': ingestion_time
    }
    
    logging.info(f"Audit log entry: {audit_entry}")
    return audit_entry

def run(argv=None):
    """Main function to run the Dataflow pipeline."""
    
    # Parse command line arguments first
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', dest='input_file', help='Input file path')
    parser.add_argument('--output_table', dest='output_table', help='Output BigQuery table')
    parser.add_argument('--temp_location', dest='temp_location', help='Temp location')
    parser.add_argument('--staging_location', dest='staging_location', help='Staging location')
    
    # Parse known args and pass the rest to pipeline options
    known_args, pipeline_args = parser.parse_known_args(argv)
    logging.info(f"Known arguments: {known_args}")
    logging.info(f"Pipeline arguments: {pipeline_args}")    
    # Create pipeline options from remaining args
    pipeline_options = PipelineOptions(pipeline_args)
    
    # Get the input file and output table
    input_file = known_args.input_file
    output_table = known_args.output_table
    temp_location = known_args.temp_location
    
    logging.info(f"Pipeline arguments:")
    logging.info(f"  Input file: {input_file}")
    logging.info(f"  Output table: {output_table}")
    logging.info(f"  Temp location: {temp_location}")
    
    if not input_file:
        raise ValueError("input_file must be specified")
    if not output_table:
        raise ValueError("output_table must be specified")
    
    # Define BigQuery schema for main data
    table_schema = {
        'fields': [
            {'name': 'entity', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'Day', 'type': 'DATE', 'mode': 'REQUIRED'},
            {'name': 'total_confirmed_deaths', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'processed_timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
        ]
    }
    
    # Define BigQuery schema for audit log
    audit_schema = {
        'fields': [
            {'name': 'file_name', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'ingestion_time', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
            {'name': 'record_count', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'input_path', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'pipeline_status', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'processed_timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
        ]
    }
    
    # Create and run the pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:
        
        # Main data processing pipeline
        processed_data = (
            pipeline
            | 'Read CSV File' >> beam.io.ReadFromText(
                input_file, 
                skip_header_lines=1
            )
            | 'Parse CSV Lines' >> beam.Map(parse_csv_line)
            | 'Filter Failed Parses' >> beam.Filter(lambda x: x is not None)
            | 'Transform Rows' >> beam.Map(transform_row)
            | 'Filter Invalid Transforms' >> beam.Filter(lambda x: x is not None)
            | 'Count Records' >> beam.ParDo(CountRecords(input_file))
        )
        
        # Write main data to BigQuery
        write_main_data = (
            processed_data
            | 'Write to BigQuery' >> WriteToBigQuery(
                table=output_table,
                schema=table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                custom_gcs_temp_location=temp_location
            )
        )
        
        # Count total records for audit log
        record_count = (
            processed_data
            | 'Count Total Records' >> beam.combiners.Count.Globally()
        )
        
        # Create audit log entry
        audit_log_entry = (
            record_count
            | 'Create Audit Log' >> beam.Map(lambda count: create_audit_log(input_file, count))
        )
        
        # Write audit log to BigQuery (separate table)
        audit_table = output_table.replace('.transformed_table', '.audit_log')
        write_audit_log = (
            audit_log_entry
            | 'Write Audit Log' >> WriteToBigQuery(
                table=audit_table,
                schema=audit_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                custom_gcs_temp_location=temp_location
            )
        )

if __name__ == '__main__':
    import sys
    run(sys.argv)
