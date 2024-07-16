'''
***********************************************************************************************************************
Description:
    * This script is designed to run in AWS Glue.
    * The main function of this script is to find out which raw files to process in DTO_Script_2 and store metadata of
      raw files.
    * Process:
        1. Raw files metadata of current glue job run is stored and its dataframe is created.
        2. Metadata of processed files i.e output of glue job DTO_Script_2 is read.
        3. By comparing processed metadata and raw metadata new_files_to_process.csv is updated with metadata of new files.
        logic for new files:
            * All new files uploaded in S3 bucket.
            * All old files which are overwritten / updated in S3 bucket and all files associated with same platform
              and month.
            * If multiple months are present in raw data, then all files associated with same month and partner.

***********************************************************************************************************************
Affected Files:
    1. raw_metadata.csv : s3://cdr-research/Projects/DTO/Metadata/raw_metadata.csv
    2. new_files_to_process.csv : s3://cdr-research/Projects/DTO/Metadata/new_files_to_process.csv

***********************************************************************************************************************
Script Call by:
    1. Scheduled Run : Script is scheduled to run daily at 5:30 AM IST.
    2. Change in data processing scripts : If changes made in GitHub - prod branch, code pipeline will autometically
       update new script in AWS S3. This change will trigger lambda function DTO_Script_1_run and execute script.
       
***********************************************************************************************************************
'''
# Importing Libraries
import sys
import pandas as pd
import numpy as np
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
import s3fs
import logging
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


# Initialize log file
log_file_path = 'glue_job_log.txt'
if not initialize_logging(log_file_path):
    upload_log_file_to_s3(log_file_path, log_file_bucket_name, log_file_key)
    raise RuntimeError("Failed to initialize logging.")

# Initialize glue job
try:
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    job.init(args['JOB_NAME'], args)
except Exception as e:
    logging.error(f"Failed to initialize Glue job: {e}")
    upload_log_file_to_s3(log_file_path, log_file_bucket_name, log_file_key)
    raise RuntimeError("Failed to initialize Glue job.") from e


       
# Create main function
def main():
    """
    This function orchestrates all above functions
    """
    try:
        # Create empty list for new raw files metadata
        new_raw_metadata = []

        # Amazon data
        read_data_from_s3_amazon(input_bucket_name, input_folder_key_amazon)
        
        # Itunes data
        read_data_from_s3_itunes(input_bucket_name, input_folder_key_itunes)
        
        # Google data
        read_data_from_s3_google(input_bucket_name, input_folder_key_google)
        
        # Get existing Metadata
        existing_metadata_df = read_existing_metadata(metadata_bucket, processed_metadata_file_key)
        
        # Current Metadata df
        current_metadata_df = create_dataframe(new_raw_metadata)
        
        # Get new files df
        new_files_df = get_new_files(existing_metadata_df, current_metadata_df)
        
        # Check if new_files_df is empty
        if new_files_df.empty:
            logging.error("No new files to process. Stopping execution.")
        else:
            # Append new metadata to existing raw metadata
            append_metadata_to_csv(current_metadata_df, metadata_bucket, raw_metadata_file_key)
            
            # upload new files dataframe to S3
            upload_new_files_to_csv(new_files_df, metadata_bucket, files_to_process_file_key)
            
            _=trigger_script_2()
            
        # Upload log file to S3
        upload_log_file_to_s3(log_file_path, log_file_bucket_name, log_file_key)
        
        logging.info("Glue job execution completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred while executing Glue job: {e}")
        raise RuntimeError("Glue job execution failed.") from e

# Call main function
if __name__ == "__main__":
    main()
