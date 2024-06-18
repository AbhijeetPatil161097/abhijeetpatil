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
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

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
import logging

# Defining necessary paths
log_file_bucket_name = 'cdr-research'
log_file_key = 'Projects/DTO/Misc/glue_job_log.txt'

raw_metadata_bucket = 'cdr-research'
raw_metadata_file_key = 'Projects/DTO/Metadata/raw_metadata.csv'

processed_metadata_bucket = 'cdr-research'
processed_metadata_file_key = 'Projects/DTO/Metadata/processed_metadata.csv'

files_to_process_bucket = 'cdr-research'
files_to_process_file_key = 'Projects/DTO/Misc/new_files_to_process.csv'

input_bucket_name = 'azv-s3str-pmsa1'

input_folder_key_amazon = 'dto_individual_partners/amazon/monthly/'
input_folder_key_itunes = 'dto_individual_partners/itunes/monthly/'
input_folder_key_google = 'dto_individual_partners/google/monthly/'


# Initialize glue job
try:
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
except Exception as e:
    logging.error(f"Failed to initialize Glue job: {e}")
    raise RuntimeError("Failed to initialize Glue job.") from e
    
    
# Initialize logging
def initialize_logging(log_file_path):
    try:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename=log_file_path)
        return True
    except Exception as e:
        print(f"An error occurred while initializing logging: {e}")
        return False
    
# Initialize log file
log_file_path = 'glue_job_log.txt'
if not initialize_logging(log_file_path):
    upload_log_file_to_s3(log_file_path, log_file_bucket_name, log_file_key)
    raise RuntimeError("Failed to initialize logging.")

# Create empty list for new raw files metadata
new_raw_metadata_amazon = []
new_raw_metadata_itunes = []
new_raw_metadata_google = []

# Function to upload log file to S3
def upload_log_file_to_s3(local_file_path, bucket_name, key):
    """
    Parameters: 
    local_file_path - log file path in glue job
    bucket_name - bucket name of log file in s3
    key - file key prefix in s3
    
    function: Uploads log file to s3 bucket
    """
    try:
        s3 = boto3.client('s3')
        s3.upload_file(local_file_path, bucket_name, key)
    except Exception as e:
        logging.error(f"An error occurred while uploading log file to S3: {e}")
        
# Function to read existing raw metadata
def read_existing_metadata(bucket_name, file_key):
    """
    Parameters: 
    bucket_name - existing metadata file bucket name
    file_key - existing metadata file's file key prefix
    
    Function:
    Reads existing metadata file and converts it into a DataFrame
    
    Returns:
    DataFrame of existing raw file metadata
    """
    s3 = boto3.client('s3')
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        existing_metadata_df = pd.read_csv(obj['Body'])
        return existing_metadata_df
    except Exception as e:
        logging.error(f"Existing metadata is empty: {e}")
        df = pd.DataFrame(columns=[
            'raw_file_path',
            'raw_file_name',
            'raw_file_creation_date',
            'raw_file_name_month',
            'platform',
            'partner',
            'raw_file_row_count',
            'months_in_data',
            'processed_file_row_count',
            'processed_date',
            'processed_file_path'
        ])										

        return df

# Function to get current iteration metadata
def create_current_metadata_df(new_raw_metadata_amazon, new_raw_metadata_itunes, new_raw_metadata_google):
    """
    Parameters:
    new_raw_metadata_amazon (list): List of dictionaries containing Amazon metadata.
    new_raw_metadata_itunes (list): List of dictionaries containing iTunes metadata.
    new_raw_metadata_google (list): List of dictionaries containing Google metadata.
    
    Function:
    Create a combined DataFrame from the metadata lists for Amazon, iTunes, and Google.
    
    Returns dataframe of current iteration metadata
    
    """
    try:
        # Create DataFrames from the lists
        amazon_df = pd.DataFrame(new_raw_metadata_amazon)
        itunes_df = pd.DataFrame(new_raw_metadata_itunes)
        google_df = pd.DataFrame(new_raw_metadata_google)
    
        # Combine the DataFrames into a single DataFrame
        current_metadata_df = pd.concat([amazon_df, itunes_df, google_df], ignore_index=True)
    
        return current_metadata_df
    except:
        logging.error(f'Failed to create current metadata dataframe')
        upload_log_file_to_s3(log_file_path, log_file_bucket_name, log_file_key)

# Get new files to process
def get_new_files(old_metadata_df, current_metadata_df):
    """
    Main function to get new files to process based on old and current metadata DataFrames.
    
    Params:
        * old_metadata_df: Metadata from previous glue job run
        * current_metadata_df: Metadata from current glue job run
    
    Logic / Steps:
        * explode_months_in_data_column: 
        If months_in_data have multiple dates, it creates copy of row for each date.
        
        * merge_dataframes: 
        Merges exploded current_metadata_df on old_metadata_df on given columns to get different rows.
        Gets only unique combinations of platform and months_in_data.
        
        * filter_matching_rows:
        Get all rows from exploded current_metadata_df associated with unique platform and months_in_data
    
    """
    
    def explode_months_in_data_column(df):
        """
        Explode the months_in_data column and creates copies of rows for each date in months_in_data
        
        Params:
            * df: current iteration metadata
        
        Returns:
        exploded_df: Exploded dataframe.
        """
        exploded_df = df.assign(months_in_data=df['months_in_data'].str.split(', ')).explode('months_in_data')
        return exploded_df
    
    def merge_dataframes(current_df, old_df):
        """
        Merge current and old DataFrames.
        
        Params:
            * current_df: Exploded dataframe from function explode_months_in_data_column
            * old_df: Dataframe from previous glue job run
        
        Returns:
        merged_df: Dataframe with all rows different rows. (current_df - old_df) 
                   based on ('raw_file_path', 'raw_file_creation_date', 'platform', 'months_in_data')
                   
        """
        on_cols = ['raw_file_path', 'raw_file_creation_date', 'platform', 'months_in_data']
        
        merged_df = current_df.merge(old_df, on=on_cols, how='left', indicator=True)
        
        merged_df = merged_df.query('_merge=="left_only"')[['months_in_data', 'platform']].drop_duplicates()
        return merged_df

    def filter_matching_rows(current_metadata_df, merged_df):
        """
        Gets all rows with unique combinations of platform and months_in_data from merged_df
        
        Params:
            * current_metadata_df: Exploded dataframe from explode_months_in_data_column
            * merged_df: Dataframe of unique combination of months_in_data and platform.
            
        Returns:
        matching_rows_df: Dataframe of all matching rows matching combination of months_in_data and platform
                          from current_metadata_df
        """
        matching_rows_df = pd.merge(current_metadata_df, merged_df, on=['months_in_data', 'platform'], how='inner')
        matching_rows_df = matching_rows_df.drop_duplicates()
        return matching_rows_df
    
    if old_metadata_df.empty:
        exploded_df = explode_months_in_data_column(current_metadata_df)
        return exploded_df
    else:
        try:
            old_metadata_df['raw_file_creation_date'] = pd.to_datetime(old_metadata_df['raw_file_creation_date']).dt.strftime('%Y-%m-%d')
            current_metadata_df['raw_file_creation_date'] = pd.to_datetime(current_metadata_df['raw_file_creation_date']).dt.strftime('%Y-%m-%d')

            exploded_df = explode_months_in_data_column(current_metadata_df)
            merged_df = merge_dataframes(exploded_df, old_metadata_df)
            matching_rows_df = filter_matching_rows(exploded_df, merged_df)
            
            return matching_rows_df
        except Exception as e:
            logging.error(f"Failed to get new_files_to_process to CSV. {e}")
            upload_log_file_to_s3(log_file_path, log_file_bucket_name, log_file_key)

# Upload new files df to S3
def upload_new_files_to_csv(new_files_df, bucket_name, file_key):
    """
    Function:
    Uploads new files DataFrame to a CSV file in an S3 bucket.

    Parameters:
    new_files_df (pd.DataFrame): DataFrame containing new files to be processed.
    bucket_name (str): The name of the S3 bucket.
    file_key (str): The S3 key (file path) for the metadata CSV file.
    
    """
    try:
        # Upload new files DataFrame to new_files_key
        csv_buffer = StringIO()
        new_files_df.to_csv(csv_buffer, index=False)
        
        s3 = boto3.client('s3')
        s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=files_to_process_file_key)
        logging.info(f"Successfully uploaded new files to CSV: {bucket_name}/{files_to_process_file_key}")
        
    except Exception as e:
        logging.error(f"An error occurred while uploading new files to CSV: {e}")
        raise

# Function to append current metadata
def append_metadata_to_csv(current_metadata_df, bucket_name, file_key):
    """
    Function:
    Uploads current metadata to S3
    
    Parameters:
    current_metadata_df- dataframe of all current metadata
    bucket_name- bucket name of file location in s3
    file_key- file key prefix for raw file metadata in s3
    
    """
    try:
        if not current_metadata_df.empty:
            csv_buffer = StringIO()
            current_metadata_df.to_csv(csv_buffer, index=False)
            s3 = boto3.client('s3')
            s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=file_key)
            logging.info(f"Successfully appended metadata to CSV: {bucket_name}/{file_key}")
        else:
            logging.info("Combined metadata rows are empty. Skipping metadata appending.")
    except Exception as e:
        logging.error(f"An error occurred while appending metadata to CSV: {e}")
        upload_log_file_to_s3(log_file_path, log_file_bucket_name, log_file_key)
        raise RuntimeError("Failed to append metadata to CSV.") from e
        
# Function to Read data from S3
# For Amazon

def read_data_from_s3_amazon(bucket_name, prefix):
    """
    Function: 
    Reads all amazon files and appends metadata to an empty list new_raw_metadata_amazon
    
    Parameters:
    bucket_name: Bucket name of raw amazon files in s3
    predix: folder prefix key for raw amazon files 
    
    """
    try:
        s3 = boto3.client('s3')
        
        # Handling all expecting file formats
        file_formats = {
        '.csv': pd.read_csv,
        '.tsv': lambda obj: pd.read_csv(obj, sep='\t'),
        '.xlsx': lambda obj: pd.read_excel(BytesIO(obj.read()), sep='\t'),
        '.txt': lambda obj: pd.read_csv(obj, sep='\t'),
        '.xls': lambda obj: pd.read_excel(BytesIO(obj.read()), sep='\t')
        }
        
        # Getting list of all available files in s3 bucket
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        # Iterate each file from s3 bucket
        for obj in response.get('Contents', []):
            file_key = obj['Key']
            # Get file Name
            file_name = os.path.basename(file_key)
            # Get file creation/modification date
            file_creation_date = obj['LastModified'].strftime('%Y-%m-%d')
            
            # Get row count
            try:
                obj = s3.get_object(Bucket=bucket_name, Key=file_key)
                file_extension = os.path.splitext(file_key)[1].lower()
                    
                # Read new files and create dataframe
                if file_extension in file_formats:
                    df = file_formats[file_extension](obj['Body'])
                else:
                    logging.error(f"Unsupported file type: {file_extension}")
                    continue
                    
                # Create Transaction_date column from file name as it is not present in data
                df['TRANSACTION_DATE'] = '-'.join(file_key.split('_')[-1].split('-')[:2])
                    
                # Append metadata
                file_row_count = len(df)
                file_creation_date = obj['LastModified'].strftime('%Y-%m-%d')
                unique_months = ','.join(df['TRANSACTION_DATE'].unique())
                try:
                    new_raw_metadata_amazon.append({
                        'raw_file_path': f"s3://{bucket_name}/{file_key}",  # S3 path
                        'raw_file_name': file_name,                         # file name
                        'raw_file_creation_date': file_creation_date,       # file modification date in s3
                        'platform': 'DTO',
                        'partner': 'amazon',
                        'raw_file_name_month': df['TRANSACTION_DATE'].unique()[0], # month from file name
                        'raw_file_row_count': file_row_count,                      # row count from file
                        'months_in_data': unique_months,                           # unique months from file
                        'num_unique_months': len(df['TRANSACTION_DATE'].unique())  # Count of unique months
                    })
                except Exception as e:
                    logging.error(f"An error occurred while appending Amazon metadata:{file_key}: {e}")
            except Exception as e:
                logging.error(f"An error occurred while processing Amazon file: {file_key}, Error: {e}")
    except Exception as e:
        logging.error(f"An error occurred while reading Amazon data: {e}")

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

        try:
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)          
            # Iterate each file
            for obj in response.get('Contents', []):
                file_key = obj['Key']
                # Get file Name
                file_name = os.path.basename(file_key)
                # Get file creation/modification date
                file_creation_date = obj['LastModified'].strftime('%Y-%m-%d')
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

                # Handle other file formats
                else:
                    try:
                        # Handle date extraction from file name
                        file_date_match = re.search(r'_(\d{4})_', file_name)
                        if file_date_match:
                            file_date_str = file_date_match.group(1)
                            file_date_formatted = '-'.join(file_date_str[i:i+2] for i in range(0, len(file_date_str), 2))
                            file_date_formatted = '20' + file_date_formatted[3:] + '-' + file_date_formatted[:2]
                        else:
                            file_date_match = re.search(r'(\d{8})', file_name)
                            if file_date_match:
                                file_date_str = file_date_match.group(1)
                                file_date = datetime.strptime(file_date_str, '%Y%m%d')
                                file_date_formatted = file_date.strftime('%Y-%m')
                            else:
                                raise ValueError("Date not found in file name.")
                        # Read and process other file formats
                        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
                        file_extension = os.path.splitext(file_key)[1].lower()
                        
                        if file_extension in file_formats:
                            df = file_formats[file_extension](obj['Body'])
                            df['FILE_NAME_DATE'] = file_date_formatted
                            df = df.dropna(subset = ['Title', 'Vendor Identifier']) 
                            # Format Begin date at a month level.
                            df['Start Date'] = pd.to_datetime(df['Start Date'], format = '%m/%d/%Y')
                            df['End Date'] = pd.to_datetime(df['End Date'], format = '%m/%d/%Y')
                            df['Start Date'] = df['Start Date'] + (df['End Date'] - df['Start Date']) / 2
                            df['Start Date'] = df['Start Date'].dt.strftime('%Y-%m')
                            
                            file_row_count = len(df)
                            # Append metadata
                            unique_months = ','.join(df['Start Date'].unique())
                            new_raw_metadata_itunes.append({
                                'raw_file_path': f"s3://{bucket_name}/{file_key}",
                                'raw_file_name': file_name,
                                'raw_file_creation_date': file_creation_date,
                                'raw_file_name_month': file_date_formatted,
                                'platform': 'DTO',
                                'partner': 'itunes',
                                'raw_file_row_count': file_row_count,
                                'months_in_data': unique_months,
                                'num_unique_months': len(df['Start Date'].unique())
                            })
                            if (df['Start Date'] == df['FILE_NAME_DATE']).all():
                                continue
                            else:
                                raise ValueError(f"Transaction Date does not match FILE_NAME_DATE for: {file_key}")
                        else:
                            logging.error(f"Unsupported file format: {file_extension}")
                    except Exception as e:
                        logging.error(f"Error processing file {file_key}: {e}")
        except Exception as e:
            logging.error(f"Error reading data from S3 bucket: {e}")
    except Exception as e:
        logging.error(f"An error occurred while reading data from S3 Itunes: {e}")
        upload_log_file_to_s3(log_file_path, log_file_bucket_name, log_file_key)
        raise RuntimeError("Failed to read data from S3 itunes.") from e  
        

# For Google
def read_data_from_s3_google(bucket_name, prefix):
    """
    Function: 
    Reads all google files and appends metadata to an empty list new_raw_metadata_google
    
    Parameters:
    bucket_name: Bucket name of raw google files in s3
    predix: folder prefix key for raw google files 
    
    """
    s3 = boto3.client('s3')

    # handle multiple file formats
    file_formats = {
        '.csv': pd.read_csv,
        '.tsv': lambda obj: pd.read_csv(obj, sep='\t'),
        '.xlsx': lambda obj: pd.read_excel(BytesIO(obj.read()), sep='\t'),
        '.txt': lambda obj: pd.read_csv(obj, sep='\t'),
        '.xls': lambda obj: pd.read_excel(BytesIO(obj.read()), sep='\t')
    }
    
    # As google data has native currency data for each country like, (USD) for US, (CAD) for CA, (GBP) for UK
    # Removing brackets from column name to integrate each curreny in single column.
    pattern = re.compile(r'\s*\(.+')
    
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        df_list_google = []
        if 'Contents' in response:
            for obj in response['Contents']:
                file_key = obj['Key']
                # Get file Name
                file_name = os.path.basename(file_key)
                # Get file creation/modification date
                file_creation_date = obj['LastModified'].strftime('%Y-%m-%d')
                try:
                    file_date_match = re.search(r'(\d{8})', file_name)
                    if file_date_match is not None:
                        file_date_str = file_date_match.group(1)
                        file_date = datetime.strptime(file_date_str, '%Y%m%d')
                    else:
                        logging.error(f"No date pattern found in filename: {file_name}")
    
                    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
                    file_extension = os.path.splitext(file_key)[1].lower()
                    if file_extension in file_formats:
                        df = file_formats[file_extension](obj['Body'], low_memory=False)

                        # As column names are not as headers check column name and mark them as headers
                        country_index = df[df['Per Transaction Report'] == 'Country'].index[0]
                        df = df.iloc[country_index:]
                        df.columns = df.iloc[0]
                        df = df[1:]
                        
                        # Remove bracket from column names
                        df.columns = [re.sub(pattern, '', col) for col in df.columns]
                        # Add a column which will have dates from file name.
                        df['FILE_NAME_DATE'] = file_date
                        df['FILE_NAME_DATE'] = df['FILE_NAME_DATE'].dt.strftime('%Y-%m')

                        # drop rows where Transaction date and Youtube Video ID is null
                        df = df.dropna(subset=['Transaction Date', 'YouTube Video ID'])
                        df['Transaction Date'] = pd.to_datetime(df['Transaction Date'])
                        df['Transaction Date'] = df['Transaction Date'].dt.strftime('%Y-%m')
                        file_row_count = len(df)
                        # Append metadata
                        unique_months = ','.join(df['Transaction Date'].unique())
                        new_raw_metadata_google.append({
                            'raw_file_path': f"s3://{bucket_name}/{file_key}",
                            'raw_file_name': file_name,
                            'raw_file_creation_date': file_creation_date,
                            'raw_file_name_month': file_date.strftime('%Y-%m'),
                            'platform': 'DTO',
                            'partner': 'google',
                            'raw_file_row_count': file_row_count,
                            'months_in_data': unique_months,
                            'num_unique_months': len(df['Transaction Date'].unique())
                        })
                        if (df['Transaction Date'] == df['FILE_NAME_DATE']).all():
                            continue
                        else:
                            logging.error(f"Transaction Date does not match FILE_NAME_DATE for: {file_key}")
                except Exception as e:
                    raise ValueError(f"An error occurred while processing file: {file_key}, Error: {e}")
        else:
            raise ValueError("No new files found in the specified folder.")
    except Exception as e:
        raise ValueError(f"No new files to process: {e}")

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
        existing_metadata_df = read_existing_metadata(processed_metadata_bucket, processed_metadata_file_key)
        
        # Current Metadata df
        current_metadata_df = create_current_metadata_df(new_raw_metadata_amazon, new_raw_metadata_itunes, new_raw_metadata_google)
        
        # Get new files df
        new_files_df = get_new_files(existing_metadata_df, current_metadata_df)
        
        # Check if new_files_df is empty
        if new_files_df.empty:
            logging.error("No new files to process. Stopping execution.")
        else:
            # Append new metadata to existing raw metadata
            append_metadata_to_csv(current_metadata_df, raw_metadata_bucket, raw_metadata_file_key)
            
            # upload new files dataframe to S3
            upload_new_files_to_csv(new_files_df, files_to_process_bucket, files_to_process_file_key)
            
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

