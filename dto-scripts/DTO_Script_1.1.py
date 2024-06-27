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
    1. Scheduled Run : Script is scheduled to run daily at 12:00 AM.
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

# Import utils.py
utils_script = s3.cat(f"s3://cdr-research/Projects/DTO/dto-scripts/utils.py").decode('utf-8')
exec(utils_script)

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


# Create empty list for new raw files metadata
new_raw_metadata = []
        

def read_data_from_s3_amazon(bucket_name, prefix):
    """
    Function to read all Amazon files from S3 and collect metadata.
    
    Parameters:
    - bucket_name: Bucket name of raw Amazon files in S3.
    - prefix: Folder prefix key for raw Amazon files.
    
    Returns:
    - List of dictionaries containing metadata information for each file.
    """
    try:
        partner = 'amazon'

        # Getting list of all available files in S3 bucket
        files = s3.find(f"{bucket_name}/{prefix}/")

        # Iterate each file from S3 bucket
        for file_path in files:
            try:
                file_key = file_path.split(f'{bucket_name}/')[1]
                file_name = os.path.basename(file_key)
                logging.info(f"Processing started for file: {file_key}")

                # Determine file extension
                file_extension = os.path.splitext(file_name)[1].lower()

                # Read file and create DataFrame
                df = _read_file_from_s3(bucket_name, file_key, file_extension)

                # Additional processing specific to your use case (e.g., creating TRANSACTION_DATE column)
                file_name_month = _extract_date_from_file_key(file_key, partner)
                df['TRANSACTION_DATE'] = file_name_month

                # Collect metadata
                file_row_count = len(df)
                file_info = s3.info(f"{bucket_name}/{file_key}")
                file_creation_date = file_info['LastModified'].strftime('%Y-%m-%d')
                unique_months = ','.join(df['TRANSACTION_DATE'].unique())

                # Append metadata to list
                _collect_file_metadata(bucket_name, 
                                       file_key, 
                                       file_name, 
                                       file_creation_date,
                                       file_name_month,
                                       partner,
                                       unique_months,
                                       file_row_count
                                      )

                logging.info(f"Raw Metadata appended for file: {file_key}")
            except:
                logging.error(f"Error reading amazon file:{file_key}")
                continue
    except Exception as e:
       logging.error(f"An error occurred while reading data from S3 Amazon: {e}")
       upload_log_file_to_s3(log_file_path, log_file_bucket_name, log_file_key)



# For Itunes
def read_data_from_s3_itunes(bucket_name, prefix):
    """
    Function: 
    Reads all itunes files and appends metadata to an empty list new_raw_metadata_itunes
    
    Parameters:
    bucket_name: Bucket name of raw itunes files in s3
    predix: folder prefix key for raw itunes files 
    
    """
    try:
        '''
        s3 = boto3.client('s3')
        
        # As Itunes has two different schemas, these schemas are matched by renaming on data.
        rename_mapping = {
            'Start Date': 'Begin Date',
            'End Date': 'End Date',
            'ISRC/ISBN': 'ISRC',
            'Quantity': 'Units',
            'Partner Share': 'Royalty Price',
            'Partner Share Currency': 'Royalty Currency',
            'Artist/Show/Developer/Author': 'Artist / Show',
            'Label/Studio/Network/Developer/Publisher': 'Label/Studio/Network',
            'ISAN/Other Identifier': 'ISAN',
            'Country Of Sale': 'Provider Country',
            'Pre-order Flag': 'PreOrder'
        }
        
        # Handling all expected file formats.
        file_formats = {
            '.csv': pd.read_csv,
            '.tsv': lambda obj: pd.read_csv(obj, sep='\t'),
            '.xlsx': lambda obj: pd.read_csv(BytesIO(obj.read()), sep='\t'),
            '.txt': lambda obj: pd.read_csv(obj, sep='\t'),
            '.xls': lambda obj: pd.read_csv(BytesIO(obj.read()), sep='\t')
        }
        '''
        try:
            partner='itunes'
            files = s3.find(f"{bucket_name}/{prefix}/")
    
            # Iterate each file from S3 bucket
            for file_path in files:
                file_key = file_path.split(f'{bucket_name}/')[1]
                file_name = os.path.basename(file_key)
                logging.info(f"Processing started for file: {file_key}")
                
                # Get file creation date using s3fs
                file_info = s3.info(file_key)
                file_creation_date = file_info['LastModified'].strftime('%Y-%m-%d')

                '''
                # Handle .gz files
                if file_key.endswith('.gz'):
                    try:
                        # Extract date from file name
                        date_match = re.search(r'(\d{8})(?:\.txt)?\.gz$', file_name)
                        if date_match:
                            file_date_str = date_match.group(1)
                            file_date = datetime.strptime(file_date_str, '%Y%m%d')
                            file_date_formatted = file_date.strftime('%Y-%m')
                        else:
                            raise ValueError("Date not found in file name.")
                            # Read and process .gz file content
                        obj_data = s3.get_object(Bucket=bucket_name, Key=file_key)['Body']
                        with gzip.open(obj_data, 'rb') as f:
                            file_content = f.read().decode('utf-8')
                            
                        # Create dataframe
                        df = pd.read_csv(StringIO(file_content), sep='\t')
                        # Add new column named FILE_NAME_DATE which will have date from file name.
                        df['FILE_NAME_DATE'] = file_date_formatted
                        # drop empty rows
                        df = df.dropna(subset = ['Title', 'Vendor Identifier'])

                        df['Begin Date'] = pd.to_datetime(df['Begin Date'], format = '%m/%d/%Y')
                        df['End Date'] = pd.to_datetime(df['End Date'], format = '%m/%d/%Y')
                        df['Begin Date'] = df['Begin Date'] + (df['End Date'] - df['Begin Date']) / 2
                        df['Begin Date'] = df['Begin Date'].dt.strftime('%Y-%m')
                        
                        file_row_count = len(df)
                        # Append metadata
                        unique_months = ','.join(df['Begin Date'].unique())
                        new_raw_metadata_itunes.append({
                            'raw_file_path': f"s3://{bucket_name}/{file_key}",
                            'raw_file_name': file_name,
                            'raw_file_creation_date': file_creation_date,
                            'raw_file_name_month': file_date.strftime('%Y-%m'),
                            'platform': 'DTO',
                            'partner': 'itunes',
                            'raw_file_row_count': file_row_count,
                            'months_in_data': unique_months,
                            'num_unique_months': len(df['Begin Date'].unique())
                        })
                        if (df['Begin Date'] == df['FILE_NAME_DATE']).all():
                            continue
                        else:
                            logging.error(f"Transaction Date does not match FILE_NAME_DATE for: {file_key}")
                    except Exception as e:
                        logging.error(f"Error processing .gz file {file_key}: {e}")
                        upload_log_file_to_s3(log_file_path, log_file_bucket_name, log_file_key)
                '''
                # Handle other file formats
                #else:
                try:
                    file_name_month = _extract_date_from_file_key(file_key, partner)
                    
                    # Read and process other file formats
                    file_extension = os.path.splitext(file_key)[1].lower()
                    df = _read_file_from_s3(bucket_name, file_key, file_extension)
                    df = df.dropna(subset = ['Title', 'Vendor Identifier'])
                    
                    if df is None:
                        logging.error(f"Itunes file is empty or could not be read: {file_key}")
                        continue
                    df['Start Date'] = pd.to_datetime(df['Start Date'], format = '%m/%d/%Y')
                    df['End Date'] = pd.to_datetime(df['End Date'], format = '%m/%d/%Y')
                        
                    # Raise error if difference between Start Date and End Date is more  than 45 Days.
                    date_diff = (df['End Date'] - df['Start Date']).dt.days
                    if (date_diff > 45).any():
                        logging.error("Start Date and End Date difference is more than 1 month.")
                            
                    df['Start Date'] = df['Start Date'] + (df['End Date'] - df['Start Date']) / 2
                    df['Start Date'] = df['Start Date'].dt.strftime('%Y-%m')
                        
                    # Get unique months from DataFrame. We are using Start Date as Transaction Date.
                    unique_months = ','.join(df['Start Date'].unique())
                    file_info = s3.info(f"{bucket_name}/{file_key}")
                    file_creation_date = file_info['LastModified'].strftime('%Y-%m-%d')
                    file_row_count = len(df)
                    
                    # Append metadata
                    unique_months = ','.join(df['Start Date'].unique())
                    _collect_file_metadata(bucket_name, 
                                       file_key, 
                                       file_name, 
                                       file_creation_date,
                                       file_name_month,
                                       partner,
                                       unique_months,
                                       file_row_count
                                      )
                    logging.info(f"Raw Metadata appended for file: {file_key}")
                    
                    if (df['Start Date'] == df['FILE_NAME_DATE']).all():
                        continue
                    else:
                        raise ValueError(f"Transaction Date does not match FILE_NAME_DATE for: {file_key}")
                except Exception as e:
                   logging.error(f"Error processing file {file_key}: {e}")
                   continue
        except Exception as e:
           logging.error(f"Error reading data from S3 bucket: {e}")           
    except Exception as e:
       logging.error(f"An error occurred while reading data from S3 Itunes: {e}")
       upload_log_file_to_s3(log_file_path, log_file_bucket_name, log_file_key)
      
    

# For Google
def read_data_from_s3_google(bucket_name, prefix):
    """
    Function: 
    Reads all google files and appends metadata to an empty list new_raw_metadata_google
    
    Parameters:
    bucket_name: Bucket name of raw google files in s3
    predix: folder prefix key for raw google files 
    
    """
    partner = 'google'
    
    # As google data has native currency data for each country like, (USD) for US, (CAD) for CA, (GBP) for UK
    # Removing brackets from column name to integrate each curreny in single column.
    pattern = re.compile(r'\s*\(.+')
    
    try:
        files = s3.find(f"{bucket_name}/{prefix}/")
        # Iterate each file from S3 bucket
        for file_path in files:
           try:
               file_key = file_path.split(f'{bucket_name}/')[1]
               file_name = os.path.basename(file_key)
               logging.info(f"Processing started for file: {file_key}")
               
               # Get file creation date using s3fs
               file_info = s3.info(file_key)
               file_creation_date = file_info['LastModified'].strftime('%Y-%m-%d')
               file_name_month = _extract_date_from_file_key(file_key, partner)
   
               # Read and process other file formats
               file_extension = os.path.splitext(file_key)[1].lower()
               df = _read_file_from_s3(bucket_name, file_key, file_extension)
               country_index = df.index[df['Per Transaction Report'] == 'Country'].tolist()[0]
               df = df.iloc[country_index:]
               df.columns = df.iloc[0]
               df = df[1:]
               
               # Convert column headers to strings explicitly
               df.columns = [re.sub(pattern, '', col) for col in df.columns]
               df = df.dropna(subset = ['Transaction Date', 'YouTube Video ID'])
               df['Transaction Date'] = pd.to_datetime(df['Transaction Date'])
               df['Transaction Date'] = df['Transaction Date'].dt.strftime('%Y-%m')
               df['QUANTITY'] = 1
               file_row_count = len(df)
               
               # Get unique months from DataFrame.
               unique_months = ','.join(df['Transaction Date'].unique())
               file_info = s3.info(f"{bucket_name}/{file_key}")
               file_creation_date = file_info['LastModified'].strftime('%Y-%m-%d')
               file_row_count = len(df)
   
               # Append metadata
               _collect_file_metadata(bucket_name, 
                                       file_key, 
                                       file_name, 
                                       file_creation_date,
                                       file_name_month,
                                       partner,
                                       unique_months,
                                       file_row_count
                                       )
               logging.info(f"Raw Metadata appended for file: {file_key}")
               
               if (df['Transaction Date'] == df['FILE_NAME_DATE']).all():
                   continue
               else:
                   logging.error(f"Transaction Date does not match FILE_NAME_DATE for: {file_key}")
           except:
              logging.error(f"Error reading amazon file:{file_key}")
              continue 
    except Exception as e:
       logging.error(f"An error occurred while reading data from S3 Google: {e}")
       upload_log_file_to_s3(log_file_path, log_file_bucket_name, log_file_key)
       

# Trigger DTO_Script_2 glue job
def trigger_script_2():
    try:
        glue_client = boto3.client('glue')
        glue_job_name = 'DTO_Script_2'
                    
        # Start the Glue job
        response = glue_client.start_job_run(JobName=glue_job_name)
        logging.info("Glue Job started:", response)
    except Exception as e:
       logging.error("Error triggering Glue job:", e)
       upload_log_file_to_s3(log_file_path, log_file_bucket_name, log_file_key)
       
       

def main():
    """
    This function orchestrates all above functions
    """
    try:
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
