# Import Libraries
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
import s3fs
import logging
import s3fs
from io import BytesIO
from io import StringIO
import sys
import boto3
import gzip
import os
import re
import pandas as pd
import numpy as np
from datetime import datetime

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
s3 = s3fs.S3FileSystem()

# Import function_defination.py
function_defination_script = s3.cat(f"s3://cdr-research/Projects/DTO/dto-scripts/function_defination.py").decode('utf-8')
exec(function_defination_script)

# Import paths.py
paths_script = s3.cat(f"s3://cdr-research/Projects/DTO/dto-scripts/paths.py").decode('utf-8')
exec(paths_script)

# Start log file    
log_file_path = 'glue_job_log.txt'  # creates a log file in script
if not initialize_logging(log_file_path):
    raise RuntimeError("Failed to initialize logging.")


# Initialize Glue Job
try:
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    job.init(args['JOB_NAME'], args)
except Exception as e:
    logging.error(f"Failed to initialize Glue job: {e}")
    upload_log_file_to_s3(log_file_path, log_file_bucket_name, log_file_key)
    raise RuntimeError("Failed to initialize Glue job.") from e


# Create main function
def main():
    try:
        # Create empty lists to store metadata
        new_raw_metadata = []
        new_processed_metadata = []
        metric_metadata = []
        metric_metadata_processed = []

        initialize_logging(log_file_path)


        for key in script_keys:
            script_content = read_script_from_s3(script_bucket_name, key)
            exec(script_content)
            logging.info(f"Scripts executed successfully for {key}.")
        
        files_to_process = read_new_files(metadata_bucket, files_to_process_file_key)
        
        df_amazon = process_amazon_data(files_to_process, input_bucket_name)
        df_itunes = process_itunes_data(files_to_process, input_bucket_name)
        df_google = process_google_data(files_to_process, input_bucket_name)
        
        final_df = merge_data(df_amazon, df_itunes, df_google)
        
        currency_df = get_currency_df(currency_bucket_name, currency_file_key)
        
        month_end_currency_data = get_last_reporting_start_date_rows(currency_df)
        
        final_df = map_conversion_rates(month_end_currency_data, final_df)
        
        final_df = map_revenue_cost_usd(final_df)
        
        final_df = reindex_dataframe(final_df)
        
        _ = write_data_to_s3(final_df, output_bucket_name, output_folder_key)
        
        _ = process_and_append_processed_metadata(new_raw_metadata, new_processed_metadata, metadata_bucket, processed_metadata_file_key)
        
        _ = process_and_append_metrics_metadata(metric_metadata, metric_metadata_processed, metadata_bucket, metric_file_key)

    except Exception as e:
        logging.error(f"An error occurred in the main script: {e}")
        upload_log_file_to_s3(log_file_path, log_file_bucket_name, log_file_key)
        raise e


# Call main function
if __name__ == "__main__":
    main()
