#!/usr/bin/env python
# coding: utf-8

# In[1]:


'''
***********************************************************************************************************************
Description:
    * This script is designed to run in AWS Glue.
    * The function of this script is read new_files_to_process.csv and get all file names and transform the data and
      load transformed data back in S3 bucket.
    * Process:
        1. new_files_to_process.csv is read and dataframe is created.
        2. Using file names in new_files_to_process.csv, amazon, itunes and google files are read and their 
           respective dataframes are created.
        3. Data processing scripts are loaded from S3 bucket and executed.
        4. Raw data is transformed individually and merged together keeping a common schema.
        5. Currency conversion rates are loaded from S3 and mapped in merged dataframe.
        6. Using currency conversion rates, REVENUE and COST values in USD are mapped in dataframe.
        7. After successful mapping, Data is written back in S3 bucket in directory VENDOR_NAME/YEAR/MONTH
        8. processed_metadata.csv is appended with metadata of all processed files.
        9. metric_validation.csv file is appended with metric values of raw and processed files.
        10. processed_metadata.csv and metric_validation.csv is uploaded in S3.
        
***********************************************************************************************************************
Affected Files:
    1. processed_metadata.csv : s3://cdr-research/Projects/DTO/Metadata/processed_metadata.csv
    2. metric_validation.csv : s3://cdr-research/Projects/DTO/Metadata/metric_validation.csv
    3. output folder : s3://cdr-research/Projects/DTO/Output

***********************************************************************************************************************
Script Call by:
    1. new_files_to_process.csv : When new_files_to_process.csv is updated, this script is executed.
    
***********************************************************************************************************************

'''

# Import Libraries
import s3fs
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

import pandas as pd
import numpy as np
from io import BytesIO, StringIO
import boto3
import gzip
import io
import re

import logging
import sys
import os

from datetime import datetime


# Define paths
log_file_bucket_name = 'cdr-research'
log_file_key = 'Projects/DTO/Misc/glue_job_log.txt'

script_bucket_name = 'cdr-research'
script_file_key_amazon = 'Projects/DTO/dto-scripts/dto_amazon_script.py'
script_file_key_itunes = 'Projects/DTO/dto-scripts/dto_itunes_script.py'
script_file_key_google = 'Projects/DTO/dto-scripts/dto_google_script.py'
integration_script_key = 'Projects/DTO/dto-scripts/dto_integration_script.py'

input_bucket_name = 'azv-s3str-pmsa1'
output_bucket_name = 'cdr-research'
output_folder_key = 'Projects/DTO/Output'

currency_bucket_name = 'cdr-research'
currency_file_key = 'Projects/DTO/Currency/data_0_0_0.csv.gz'

new_files_bucket_name = 'cdr-research'
new_files_file_key = 'Projects/DTO/Misc/new_files_to_process.csv'

metric_bucket_name = 'cdr-research'
metric_file_key = 'Projects/DTO/Metadata/metric_validation.csv'

processed_metadata_bucket = 'cdr-research'
processed_metadata_file_key = 'Projects/DTO/Metadata/processed_metadata.csv'


# Create empty lists to store metadata
new_raw_metadata_amazon = []
new_raw_metadata_itunes = []
new_raw_metadata_google = []
new_processed_metadata_all = []

metric_metadata_amazon = []
metric_metadata_itunes = []
metric_metadata_google = []
metric_metadata = []


# Initialize logging
def initialize_logging(log_file_path):
    """
    Function:
        * Initializes logging file.
    
    Parameters:
        * log_file_path : Path of log file
    
    """
    try:
        logging.basicConfig(
            level=logging.INFO, 
            format='%(asctime)s - %(levelname)s - %(message)s', 
            filename=log_file_path
        )
        return True
    except Exception as e:
        print(f"An error occurred while initializing logging: {e}")
        return False
    
    
# Start log file    
log_file_path = 'glue_job_log.txt'  # creates a log file in script
if not initialize_logging(log_file_path):
    raise RuntimeError("Failed to initialize logging.")
    
    
# Initialize Glue Job
try:
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
except Exception as e:
    logging.error(f"Failed to initialize Glue job: {e}")
    upload_log_file_to_s3(log_file_path, log_file_bucket_name, log_file_key)
    raise RuntimeError("Failed to initialize Glue job.") from e
    
    
# Upload log file to s3
def upload_log_file_to_s3(local_file_path, bucket_name, key):
    """
    Function:
        * Uploads log file to S3 bucket
    
    Parameters:
        * local_file_path: path of log file in Glue job
        * bucket_name: Log file output bucket name
        * key: Output log File prefix.
    
    """
    try:
        s3 = s3fs.S3FileSystem()
        s3.put(local_file_path, f"{bucket_name}/{key}")
    except Exception as e: 
        logging.error(f"An error occurred while uploading log file to S3: {e}")
        
        
# Read new files to process
def read_new_files(bucket_name, file_key):
    """
    Function:
        * Reads existing new file to process from S3 and converts it into a DataFrame.
    
    Parameters:
        * bucket_name: S3 bucket name of files_to_process.csv.
        * file_key: S3 key for the files_to_process.csv.
    
    Returns:
        * DataFrame: DataFrame of new files to process.
    """
    try:
        s3_url = f"s3://{bucket_name}/{file_key}"
        files_to_process = pd.read_csv(s3_url)
        return files_to_process
    except Exception as e:
        logging.error(f"An error occurred in function read_new_files: {e}")
        raise RuntimeError("Failed to run function read_new_files .") from e
        
        
# Read script from S3
def read_script_from_s3(bucket_name, file_key):
    """
    Function:
        * Reads scripts from S3 bucket
    
    Parameters:
        * bucket_name: S3 bucket name of script location.
        * file_key: S3 key for the script.
    
    Returns:
        * Script in original format.
    """
    try:
        s3 = s3fs.S3FileSystem()
        script = s3.cat(f"s3://{bucket_name}/{file_key}").decode('utf-8')
        return script
    except Exception as e:
        logging.error(f"An error occurred while reading script from S3: {e}")
        raise RuntimeError("Failed to run function read_script_from_s3.") from e
        

# Read Amazon Data from S3 and convert in dataframe      
def read_data_from_s3_amazon(files_to_process, bucket_name):
    """
    Function:
        * Reads Amazon files from S3 listed in the files_to_process and processes them into a DataFrame.
    
    Parameters:
        * files_to_process: DataFrame containing metadata of new files to process from function read_new_files.
        * bucket_name: S3 bucket name of raw Amazon Data.
    
    Returns:
        * DataFrame: Combined DataFrame from all Amazon files from read_new_files.
    """
    def read_file_from_s3(file_key, file_extension):
        """
        Reads a file from S3 based on the file key and file extension.

        """
        try:
            s3 = s3fs.S3FileSystem()
            file_path = f"s3://{bucket_name}/{file_key}"
            
            if file_extension == '.csv':
                return pd.read_csv(file_path)
            
            elif file_extension == '.tsv':
                return pd.read_csv(file_path, sep='\t')
            
            elif file_extension in ('.xlsx'):
                return pd.read_excel(file_path)
            
            else:
                raise ValueError(f"Unsupported file format in Amazon Data: {file_extension}")
                
        except Exception as e:
            logging.error(f"An error occurred while reading Amazon file from S3: {e}")
            return None
    
    # Extract Metadata of file
    def extract_file_metadata(file_key):
        """
        Extracts metadata of a file from S3.

        """
        try:
            s3 = s3fs.S3FileSystem()
            file_info = s3.info(f"{bucket_name}/{file_key}")
            file_creation_date = file_info['LastModified'].strftime('%Y-%m-%d')
            return file_creation_date
        except Exception as e:
            logging.error(f"An error occurred while extracting file metadata: {e}")
            return None

        
    try:
        s3 = s3fs.S3FileSystem()
        # Filter to get all Amazon Files
        amazon_files_df = files_to_process[files_to_process['platform'] == 'Amazon']
        
        # Check of amazon_files_df is empty.
        if amazon_files_df.empty:
            logging.error("No new Amazon files to process.")
            return pd.DataFrame()
        
        # Empty list to store amazon dataframes
        df_list_amazon = []
        
        for index, row in amazon_files_df.iterrows():
            try:
                s3_url = row['raw_file_path']
                file_key = s3_url.split(f's3://{bucket_name}/')[1]
                file_name = os.path.basename(file_key)
                file_extension = os.path.splitext(file_key)[1].lower()
                df = read_file_from_s3(file_key, file_extension)
                
                if df is None:
                    logging.error(f"Amazon file is empty: {file_key}")
                    continue
                    
                # Get Metadata
                file_creation_date = extract_file_metadata(file_key)
                file_row_count = len(df)
                
                # Extract TRANSACTION_DATE from the file key if present
                df['TRANSACTION_DATE'] = '-'.join(file_key.split('_')[-1].split('-')[:2])
                unique_months = ','.join(df['TRANSACTION_DATE'].unique())
                
                # Append metadata for processed files
                try:
                    new_raw_metadata_amazon.append({
                        'raw_file_path': f"s3://{bucket_name}/{file_key}", 
                        'raw_file_name': file_name,                       
                        'raw_file_creation_date': file_creation_date,
                        'raw_file_name_month': unique_months,
                        'platform': 'Amazon',
                        'partner': 'DTO',
                        'months_in_data': unique_months,
                        'raw_file_row_count': file_row_count
                    })
                    
                except Exception as e:
                    logging.error(
                        f"Failed to append Amazon metadata in list new_raw_metadata_amazon:{file_key}, Error {e}"
                    )
                
                # Append metadata for metric validation
                try:
                    metric_metadata_amazon.append({
                        'raw_file_path': f"s3://{bucket_name}/{file_key}", 
                        'raw_file_name': file_name,                       
                        'platform': 'Amazon',
                        'partner': 'DTO',
                        'months_in_data': unique_months,
                        'metric': [
                            'QUANTITY (Quantity)', 
                            'COST_NATIVE (Cost)'
                            ],
                        
                        'raw_file_value': [
                            df['Quantity'].astype('float').sum(), 
                            df['Cost'].astype('float').sum()
                            ]
                    })
                    
                except Exception as e:
                    logging.error(
                        f"Failed to append Amazon metadata in list metric_metadata_amazon:{file_key}, Error {e}"
                    )
                    
                # Append df in df_list_amazon
                df_list_amazon.append(df)
                
            except Exception as e:
                logging.error(f"An error occurred while processing Amazon file: {file_key}, Error: {e}")
        
        # Concat dataframe from df_list_amazon.        
        df_amazon = pd.concat(df_list_amazon, ignore_index=True)
        df_amazon.drop_duplicates()
        
        return df_amazon  
        logging.info(f"Amazon Dataframe created successfully")
    except Exception as e:
        logging.error(f"An error occurred while reading Amazon data: {e}")
        return pd.DataFrame()    

# Itunes data reading function
def read_data_from_s3_itunes(files_to_process, bucket_name):
    """
    Function:
        * Reads iTunes files from S3 listed in the files_to_process and processes them into a DataFrame.
    
    Parameters:
        * files_to_process: DataFrame of metadata of new files to process.
        * bucket_name: S3 bucket name.
    
    Returns:
        * DataFrame: Combined DataFrame containing data from all iTunes files.
    """
    
    def read_file_from_s3(file_key, file_extension):
        """
        Reads a file from S3 based on the file key and file extension.

        """
        try:
            s3 = s3fs.S3FileSystem()
            file_path = f"s3://{bucket_name}/{file_key}"
            
            if file_extension == '.csv':
                return pd.read_csv(file_path)
            
            elif file_extension == '.tsv':
                return pd.read_csv(file_path, sep='\t')
            
            elif file_extension in ('.xls'):
                return pd.read_csv(file_path, sep='\t')
            
            elif file_extension in ('.gz'):
                with s3.open(file_path, 'rb') as f:
                    return pd.read_csv(f, sep='\t', compression='gzip')
                    
            elif file_extension == '.txt':
                return pd.read_csv(file_path, sep='\t')
            
            else:
                raise ValueError(f"Unsupported file format in iTunes Data: {file_extension}")
                
        except Exception as e:
            logging.error(f"An error occurred while reading iTunes file from S3: {e}")
            return None
        
        # Ectract metadata of file
    def extract_file_metadata(file_key):
        """
        Extracts metadata of a file from S3.

        """
        try:
            s3 = s3fs.S3FileSystem()
            file_info = s3.info(f"{bucket_name}/{file_key}")
            file_creation_date = file_info['LastModified'].strftime('%Y-%m-%d')
            return file_creation_date
        except Exception as e:
            logging.error(f"An error occurred while extracting file metadata: {e}")
            return None
        
        
    try:
        # As there are multiple schemas in iTunes data. We are renaming column names to match schemas.
        rename_mapping = {
            'Start Date': 'Begin Date',
            'ISRC/ISBN': 'ISRC',
            'Quantity': 'Units',
            'Partner Share': 'Royalty Price',
            'Partner Share Currency': 'Royalty Currency',
            'Artist/Show/Developer/Author': 'Artist / Show',
            'Label/Studio/Network/Developer/Publisher': 'Label/Studio/Network',
            'ISAN/Other Identifier': 'ISAN',
            'Country Of Sale': 'Country Code',
            'Pre-order Flag': 'PreOrder'
        }
        
        # Filter the files_to_process for iTunes platform
        itunes_files_df = files_to_process[files_to_process['platform'] == 'Itunes']
        
        if itunes_files_df.empty:
            logging.warning("No new iTunes files to process.")
            return pd.DataFrame()
        
        # Create empty list to store dataframes of each file
        df_list_gz = []
        df_list_others = []
        
        for index, row in itunes_files_df.iterrows():
            try:
                s3_url = row['raw_file_path']
                file_key = s3_url.split(f's3://{input_bucket_name}/')[1]
                file_name = os.path.basename(file_key)
                file_extension = os.path.splitext(file_key)[1]
                file_creation_date = extract_file_metadata(file_key)
                try:
                    if file_key.endswith('.gz'):
                        date_match = re.search(r'(\d{8})\.txt\.gz$|(\d{8})\.gz$', file_name)
                        if date_match:
                            file_date_str = date_match.group(1) or date_match.group(2)
                            file_date = datetime.strptime(file_date_str, '%Y%m%d')
                            raw_file_name_month = file_date.strftime('%Y-%m')
                        else:
                            raise ValueError("Date not found in file name.")
                        
                        df = read_file_from_s3(file_key, file_extension)
                        df = df.dropna(subset = ['Title', 'Vendor Identifier'])
                        file_row_count = len(df)
                        
                        df['Begin Date'] = pd.to_datetime(df['Begin Date'], format = '%m/%d/%Y')
                        df['End Date'] = pd.to_datetime(df['End Date'], format = '%m/%d/%Y')
                        
                        # Raise error if difference between Start Date and End Date is more  than 45 Days.
                        date_diff = (df['End Date'] - df['Begin Date']).dt.days
                        if (date_diff > 45).any():
                            raise RuntimeError("Begin Date and End Date difference is more than 1 month.")
                            
                        df['Begin Date'] = df['Begin Date'] + (df['End Date'] - df['Begin Date']) / 2
                        df['Begin Date'] = df['Begin Date'].dt.strftime('%Y-%m')
                        
                        unique_months = ','.join(df['Begin Date'].unique())
                        try:
                            new_raw_metadata_itunes.append({
                                'raw_file_path': f"s3://{bucket_name}/{file_key}", 
                                'raw_file_name': file_name,                       
                                'raw_file_creation_date': file_creation_date,
                                'raw_file_name_month': raw_file_name_month,
                                'platform': 'Itunes',
                                'partner': 'DTO',
                                'months_in_data': unique_months,
                                'raw_file_row_count': file_row_count

                            })
                        except Exception as e:
                            logging.error(
                                f"Failed to append iTunes metadata in list new_raw_metadata_itunes:{file_key}, Error {e}"
                            )
                            
                        try:   
                            metric_metadata_itunes.append({
                                'raw_file_path': f"s3://{bucket_name}/{file_key}", 
                                'raw_file_name': file_name,                       
                                'platform': 'Itunes',
                                'partner': 'DTO',
                                'months_in_data': unique_months,
                                'metric': 'QUANTITY (Units)',
                                'raw_file_value': df['Units'].sum()
                            })
                        except Exception as e:
                            logging.error(
                                f"Failed to append iTunes metadata in list metric_metadata_itunes:{file_key}, Error {e}"
                            )
                        # Append dataframes to df_list_gz
                        df_list_gz.append(df)
                        
                    else:
                        try:
                            file_date_match = re.search(r'_(\d{8})', file_name)
                                
                            if file_date_match:
                                file_date_str = file_date_match.group(1)
                                file_date = datetime.strptime(file_date_str, '%Y%m%d')
                                raw_file_name_month = file_date.strftime('%Y-%m')
                            else:
                                # Try to match a date in the format _MMYY_ in the file name
                                file_date_match = re.search(r'_(\d{2})(\d{2})_', file_name)
                                
                                if file_date_match:
                                    month_str = file_date_match.group(1)
                                    year_str = file_date_match.group(2)
                                    raw_file_name_month = f"20{year_str}-{month_str}"
                                else:
                                    raise ValueError("Date not found in file name.")
                                
                            df = read_file_from_s3(file_key, file_extension)
                            df = df.dropna(subset = ['Title', 'Vendor Identifier'])
                            file_row_count = len(df)
                            
                            df['Start Date'] = pd.to_datetime(df['Start Date'], format = '%m/%d/%Y')
                            df['End Date'] = pd.to_datetime(df['End Date'], format = '%m/%d/%Y')
                            
                            # Raise error if difference between Start Date and End Date is more  than 45 Days.
                            date_diff = (df['End Date'] - df['Start Date']).dt.days
                            if (date_diff > 45).any():
                                raise RuntimeError("Start Date and End Date difference is more than 1 month.")
                                
                            df['Start Date'] = df['Start Date'] + (df['End Date'] - df['Start Date']) / 2
                            df['Start Date'] = df['Start Date'].dt.strftime('%Y-%m')
                            
                            unique_months = ','.join(df['Start Date'].unique())
                            try:
                                new_raw_metadata_itunes.append({
                                    'raw_file_path': f"s3://{bucket_name}/{file_key}", 
                                    'raw_file_name': file_name,                       
                                    'raw_file_creation_date': file_creation_date,
                                    'raw_file_name_month': raw_file_name_month,
                                    'platform': 'Itunes',
                                    'partner': 'DTO',
                                    'months_in_data': unique_months,
                                    'raw_file_row_count': file_row_count
                                })
                            except Exception as e:
                                logging.error(
                                    f"Failed to append iTunes metadata in list new_raw_metadata_itunes:{file_key}, Error {e}"
                                )
                            try:
                                metric_metadata_itunes.append({
                                    'raw_file_path': f"s3://{bucket_name}/{file_key}", 
                                    'raw_file_name': file_name,                       
                                    'platform': 'Itunes',
                                    'partner': 'DTO',
                                    'months_in_data': unique_months,
                                    'metric': 'QUANTITY (Quantity)',
                                    'raw_file_value': df['Quantity'].sum()
                                })
                            except Exception as e:
                                logging.error(
                                    f"Failed to append iTunes metadata in list metric_metadata_itunes:{file_key}, Error {e}"
                                )
                                
                            df_list_others.append(df)
                        except Exception as e:
                            logging.error(f"Failed to read other itunes file formats:{file_key}, Error {e}") 
                                      
                except Exception as e:
                    logging.error(f"file format not compatible: {file_key}, Error: {e}")     
            except Exception as e:
                logging.error(f"An error occurred while processing iTunes file: {file_key}, Error: {e}")            
            
        if not len(df_list_gz) == 0:
            df_1 = pd.concat(df_list_gz, ignore_index=True)
        else:
            logging.info(f"df_1 is empty")
            df_1 = pd.DataFrame()
        if not len(df_list_others) == 0:
            df_2 = pd.concat(df_list_others, ignore_index=True)
            df_2_renamed = df_2.rename(columns=rename_mapping)
        else:
            logging.info(f"df_2_renamed is empty")
            df_2_renamed = pd.DataFrame()

        # Combine both dataframes.
        df_itunes = pd.concat([df_1, df_2_renamed], ignore_index=True)
        
        # Adding empty columns to prevent errors while running data processing scripts.
        column_name = ['Asset/Content Flavor', 'Primary Genre', 'Provider Country', 'Sales or Return']
        for i in column_name:
            if i not in df_itunes.columns:
                df_itunes[i] = None
        
        df_itunes.drop_duplicates()
        return df_itunes
        logging.info(f"Itunes dataframe created successfully")
    
    except Exception as e:
        logging.error(f"An error occurred while reading iTunes data: {e}")
        return pd.DataFrame()

    
# Google data reading function
def read_data_from_s3_google(files_to_process, bucket_name):
    """
    Function:
        * Reads Google files from S3 listed in the files_to_process and processes them into a DataFrame.
    
    Parameters:
        * files_to_process: DataFrame of metadata of new files to process.
        * bucket_name: S3 bucket name of raw Google data.
    
    Returns:
        * DataFrame: Combined DataFrame containing data from all Google files.
    """
    
    def read_file_from_s3(file_key, file_extension):
        """
        Reads a file from S3 based on the file key and file extension.

        """
        try:
            s3 = s3fs.S3FileSystem()
            file_path = f"s3://{bucket_name}/{file_key}"
            
            if file_extension == '.csv':
                return pd.read_csv(file_path)
            
            elif file_extension == '.tsv':
                return pd.read_csv(file_path, sep='\t')
            
            elif file_extension in ('.xlsx'):
                return pd.read_excel(file_path)
            
            elif file_extension in ('.gz'):
                with s3.open(file_path, 'rb') as f:
                    return pd.read_csv(f, sep='\t', compression='gzip')
            
            else:
                raise ValueError(f"Unsupported file format in iTunes Data: {file_extension}")
                
        except Exception as e:
            logging.error(f"An error occurred while reading iTunes file from S3: {e}")
            return None
        
    # Ectract metadata of file
    def extract_file_metadata(file_key):
        """
        Extracts metadata of a file from S3.

        """
        try:
            s3 = s3fs.S3FileSystem()
            file_info = s3.info(f"{bucket_name}/{file_key}")
            file_creation_date = file_info['LastModified'].strftime('%Y-%m-%d')
            return file_creation_date
        except Exception as e:
            logging.error(f"An error occurred while extracting file metadata: {e}")
            return None
    
    try:
        google_files_df = files_to_process[files_to_process['platform'] == 'Google']
        
        if google_files_df.empty:
            logging.warning("No new Google files to process.")
            return pd.DataFrame()
        
        pattern = re.compile(r'\s*\(.+')
        # Create an empty list to store DataFrames
        df_list_google = []
        
        for index, row in google_files_df.iterrows():
            try:
                s3_url = row['raw_file_path']
                file_key = s3_url.split(f's3://{input_bucket_name}/')[1]
                file_name = os.path.basename(file_key)
                file_extension = os.path.splitext(file_key)[1]
                file_creation_date = extract_file_metadata(file_key)
                
                # Get date from file name
                file_date_match = re.search(r'(\d{8})', file_name)
                file_date_str = file_date_match.group(1)
                file_date = datetime.strptime(file_date_str, '%Y%m%d')
                
                try:
                    df = read_file_from_s3(file_key, file_extension)
                    
                    if df is None:
                        logging.error(f"Google file is empty: {file_key}")
                        continue
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
                    unique_months = ','.join(df['Transaction Date'].unique())
                    try:
                        new_raw_metadata_google.append({
                            'raw_file_path': f"s3://{bucket_name}/{file_key}", 
                            'raw_file_name': file_name,                       
                            'raw_file_creation_date': file_creation_date,
                            'raw_file_name_month': file_date.strftime('%Y-%m'),
                            'platform': 'Google',
                            'partner': 'DTO',
                            'months_in_data': unique_months,
                            'raw_file_row_count': file_row_count
                        })
                    except Exception as e:
                        logging.error(
                            f"Failed to append iTunes metadata in list new_raw_metadata_google:{file_key}, Error {e}"
                        )
                    try:
                        metric_metadata_google.append({
                            'raw_file_path': f"s3://{bucket_name}/{file_key}", 
                            'raw_file_name': file_name,                       
                            'platform': 'Google',
                            'partner': 'DTO',
                            'months_in_data': unique_months,
                            'metric': [
                                'QUANTITY (QUANTITY)',
                                'REVENUE_NATIVE (Native Retail Price)'
                                ],
                            
                            'raw_file_value': [
                                df['QUANTITY'].astype('float').sum(), 
                                df['Native Retail Price'].astype('float').sum()
                                ]
                        })
                    except Exception as e:
                        logging.error(
                            f"Failed to append Google metadata in list metric_metadata_google:{file_key}, Error {e}"
                        ) 
                        
                    df_list_google.append(df)
                except Exception as e:
                    logging.error(f"An error occurred while creating Google dataframe: {file_key}, Error: {e}")
            except Exception as e:
                    logging.error(f"An error occurred while reading Google file: {file_key}, Error: {e}")
                    
        df_google = pd.concat(df_list_google, ignore_index=True)
        df_google.drop_duplicates()
        
        return df_google
        logging.info(f"Google Dataframe created successfully")
    
    except Exception as e:
        logging.error(f"An error occurred while reading Google data: {e}")
        return pd.DataFrame()
   
# Write data to s3  
def write_data_to_s3(df, bucket_name, file_key):
    """
    Function:
        * Writes processed data to S3 bucket in VENDOR_NAME > YEAR > MONTH format.
        * Appends processed metadata into list
    
    Parameters:
        * df: DataFrame of processed / transformed data.
        * bucket_name: S3 bucket of output
        * file_key: File prefix of output folder.
        
    """

    enable_overwrite = False
    try:
        s3 = s3fs.S3FileSystem()
        for vendor_name, group in df.groupby('VENDOR_NAME'):
            for (year, month), data in group.groupby([
                pd.to_datetime(group['TRANSACTION_DATE']).dt.year,
                pd.to_datetime(group['TRANSACTION_DATE']).dt.month
            ]):
                # Define file name and key
                file_name = f"vendor_name={vendor_name}/year={year}/month={vendor_name}_{year}_{month}.csv"
                file_key_name = f"{file_key}/{file_name}"
                platform_name = vendor_name.capitalize()
                
                # Collect metadata for this file
                transaction_date = f"{year}-{month:02d}"
                new_processed_metadata_all.append({
                    'processed_file_row_count': len(data),
                    'processed_date': datetime.now().strftime('%Y-%m-%d'),
                    'processed_file_path': f"s3://{bucket_name}/{file_key_name}",
                    'months_in_data': transaction_date,
                    'platform': platform_name
                })
                try:
                    if vendor_name == 'AMAZON':
                        metric_metadata.append({
                            'processed_file_value': [data['QUANTITY'].sum(), 
                                                     data['COST_NATIVE'].sum()
                                                    ],
                            'processed_date': datetime.now().strftime('%Y-%m-%d'),
                            'months_in_data': transaction_date,
                            'platform': platform_name
                        })
                    elif vendor_name == 'iTUNES':
                        try:
                            metric_metadata.append({
                                'processed_file_value': data['QUANTITY'].sum(),
                                'processed_date': datetime.now().strftime('%Y-%m-%d'),
                                'months_in_data': transaction_date,
                                'platform': platform_name,
                            })
                        except:
                            logging.error(f"An error occurred while writing itunes metric data to S3: {e}")
                    elif vendor_name == 'GOOGLE':
                        metric_metadata.append({
                            'processed_file_value': [data['QUANTITY'].sum(),
                                                     data['REVENUE_NATIVE'].sum()
                                                    ],
                            'processed_date': datetime.now().strftime('%Y-%m-%d'),
                            'months_in_data': transaction_date,
                            'platform': platform_name
                        })

                except Exception as e:
                    logging.error(f"An error occurred while writing metric data to S3: {e}")
                    raise RuntimeError("Failed to write data to S3.") from e
        path = f's3://{bucket_name}/{file_key_name}'
        data.to_csv(path, index=False, mode='w')
        logging.info(f"Data writing to S3 is successful.")
    except Exception as e:
        logging.error(f"An error occurred while writing data to S3: {e}")
        raise RuntimeError("Failed to write data to S3.") from e
        
        
# concat processed metadata
def concat_metadata(raw_metadata, processed_metadata):
    """
    Function:
        * Concatenates processed files DataFrame with metadata DataFrame based on transaction date and platform.

    Parameters:
        * processed_files_df: DataFrame of processed files with metadata.
        * metadata_df: DataFrame containing metadata for processed files.

    Returns:
        * DataFrame: Merged DataFrame containing combined information.
    """
    # Merge processed files DataFrame with metadata DataFrame based on transaction_date and platform
    new_processed_metadata = raw_metadata.merge(processed_metadata, on=['months_in_data', 'platform'], how='left')
    
    return new_processed_metadata



# Get currency data of last date of month of each currency
def get_last_reporting_start_date_rows(df):
    """
    Function:
        * Gets only the last reported data of each month from currency data.
    
    Parameters:
        * df: DataFrame containing currency data.
    
    Returns:
        * DataFrame: Rows with last reported data of each month.
    """
    try:
        df['REPORTING_START_DATE'] = pd.to_datetime(df['REPORTING_START_DATE'])
        df = df.sort_values(by=['COUNTRY_CODE', 'REPORTING_START_DATE'])
        result_df = df.groupby(['COUNTRY_CODE', df['REPORTING_START_DATE'].dt.to_period('M')]).apply(lambda x: x.tail(1)).reset_index(drop=True)
        result_df['REPORTING_START_DATE'] = result_df['REPORTING_START_DATE'].dt.strftime('%m-%Y')
        return result_df
    except Exception as e:
        logging.error(f"An error occurred while getting last reporting start date rows: {e}")


def map_conversion_rates(month_end_currency_data, final_df):
    """
    Function:
        * Maps conversion rates to all transformed data. ( Amazon + iTunes + Google)
    
    Parameters:
        * month_end_currency_data: Month end data from function get_last_reporting_start_date_rows.
        * final_df: All transformed partner data of ( Amazon + iTunes + Google)
    
    Returns:
        * Dataframe of all transformed partner data with newly mapped conversion rates.
    """
    try:
        # Use conversion rates from snowflake to standardize the flow.
        conversion_map = {(date, country): conversion_rate for date, country, conversion_rate in zip(
            month_end_currency_data['REPORTING_START_DATE'], 
            month_end_currency_data['COUNTRY_CODE'], 
            month_end_currency_data['CONVERSION_RATE']
        )}

            # Map conversion rates based on date and country code
        final_df['CONVERSION_RATE'] = final_df.apply(lambda row: conversion_map.get((row['TRANSACTION_DATE'], 'GB') if row['TERRITORY'] == 'UK' else (row['TRANSACTION_DATE'], row['TERRITORY']), None), axis=1)
        return final_df
        logging.info(f"Conversion rates mapping is successful.")
    except Exception as e:
        logging.error(f"An error occurred while mapping revenue USD: {e}")
        raise RuntimeError("Failed to map conversion rates.") from e
        upload_log_file_to_s3(log_file_path, log_file_bucket_name, log_file_key)
        
        
# Map values of revenue_usd and cost_usd to dataframe
def map_revenue_cost_usd(df):
    """
    Function:
        * Maps revenue and cost in USD in Dataframe
    
    Parameters:
        * df: Dataframe from output of function (map_conversion_rates) after mapping conversion rates . 
    
    Returns:
        * Dataframe after mapping revenue and cost in usd.
        
    """
    try:
        df['REVENUE_USD'] = df['REVENUE_NATIVE'] * df['CONVERSION_RATE']
        df['COST_USD'] = df['COST_NATIVE'] * df['CONVERSION_RATE']
        return df
        logging.info(f"Revenue and Cost in USD mapping is successful.")
        
    except Exception as e:
        logging.error(f"An error occurred while mapping revenue USD: {e}")
        raise RuntimeError("Failed to map revenue and cost in USD.") from e
        upload_log_file_to_s3(log_file_path, log_file_bucket_name, log_file_key)
        
#         
def raw_metadata(new_raw_metadata_amazon, new_raw_metadata_itunes, new_raw_metadata_google):
    """
    Function:
        * Creates a combined DataFrame from the metadata lists for Amazon, iTunes, and Google.
    
    Parameters:
        * new_raw_metadata_amazon: List of dictionaries containing Amazon metadata.
        * new_raw_metadata_itunes): List of dictionaries containing iTunes metadata.
        * new_raw_metadata_google: List of dictionaries containing Google metadata.
    
    Returns:
        * DataFrame: DataFrame of current iteration metadata.
    """
    amazon_df = pd.DataFrame(new_raw_metadata_amazon)
    itunes_df = pd.DataFrame(new_raw_metadata_itunes)
    google_df = pd.DataFrame(new_raw_metadata_google)
    
    raw_metadata = pd.concat([amazon_df, itunes_df, google_df], ignore_index=True)
    return raw_metadata


def append_metadata_to_csv(new_processed_metadata, bucket_name, file_key):
    """
    Function:
        * If file already present, Appends current processed metadata DataFrame to a CSV file in an S3 bucket.
        * If file not present, writes current processed metadata DataFrame to a CSV file in an S3 bucket.
    
    Parameters:
        * current_metadata_df): DataFrame containing current metadata.
        * bucket_name: S3 bucket name.
        * file_key: S3 key for the metadata CSV.
    """
    path = f's3://{bucket_name}/{file_key}'
    fs = s3fs.S3FileSystem()

    def file_exists():
        """
        Check if the file exists in the S3 bucket.
        
        """
        return fs.exists(path)

    def get_existing_columns():
        """
        Get the columns from the existing file in S3.
        
        """
        old_metadata = pd.read_csv(path)
        return old_metadata.columns.tolist()

    def reorder_columns(new_metadata, column_order):
        """
        Reorder the columns of the new metadata to match the existing file's column order.
        
        """
        return new_metadata[column_order]

    def write_data(new_metadata):
        """
        Write the new metadata to the file in S3.
        
        """
        new_metadata.to_csv(path, index=False)
        logging.info(f"Writing metadata is successful. {path}")

    def append_data(new_metadata):
        """
        Append the new metadata to the existing file in S3.
        
        """
        new_metadata.to_csv(path, index=False, mode='a', header=False)
        logging.info(f"Appending metadata is successful. {path}")

    try:
        if file_exists():
            # If File exists, get existing columns and reorder new data
            existing_columns = get_existing_columns()
            reordered_metadata = reorder_columns(new_processed_metadata, existing_columns)
            append_data(reordered_metadata)
        else:
            # If File does not exist, write new data
            write_data(new_processed_metadata)
    except Exception as e:
        logging.error(f"An error occurred while uploading metadata: {e}")
    
    
try:
    # Read the Python scripts and integration script from S3
    script_content_amazon = read_script_from_s3(script_bucket_name, script_file_key_amazon)
    script_content_itunes = read_script_from_s3(script_bucket_name, script_file_key_itunes)
    script_content_google = read_script_from_s3(script_bucket_name, script_file_key_google)
    integration_script_content = read_script_from_s3(script_bucket_name, integration_script_key)

    # Execute the Python scripts
    exec(script_content_amazon)
    exec(script_content_itunes)
    exec(script_content_google)
    exec(integration_script_content)
    
    # read new files to process
    files_to_process = read_new_files(new_files_bucket_name, new_files_file_key)
                                                
    # Read and process data (if no data to process return empty dataframe)
    try:
        df_amazon = read_data_from_s3_amazon(files_to_process, input_bucket_name)
        amazon_monthly_data = DtoDataProcessAmazon('Amazon', df_amazon)
        df_transformed_amazon = amazon_monthly_data.process_data_source()
        logging.info(f"Amazon Data Processing success")
    except Exception as e:
        logging.error(f"An error occurred during Amazon data processing: {e}")
        df_transformed_amazon = pd.DataFrame()
        
    try:
        df_itunes = read_data_from_s3_itunes(files_to_process, input_bucket_name)
        itunes_monthly_data = DtoDataProcessItunes('Itunes', df_itunes)
        df_transformed_itunes = itunes_monthly_data.process_data_source()
        logging.info(f"Itunes Data Processing success")
    except Exception as e:
        logging.error(f"An error occurred during iTunes data processing: {e}")
        df_transformed_itunes = pd.DataFrame()
        
    try:
        df_google = read_data_from_s3_google(files_to_process, input_bucket_name)
        google_monthly_data = DtoDataProcessGoogle('Google', df_google)
        df_transformed_google = google_monthly_data.process_data_source()
        logging.info(f"Google Data Processing success")
    except Exception as e:
        logging.error(f"An error occurred during Google data processing: {e}")
        df_transformed_google = pd.DataFrame()


    # Merge Dataframes
    try:
        final_df = merge_dataframes(
            df_amazon = df_transformed_amazon, 
            df_itunes = df_transformed_itunes, 
            df_google = df_transformed_google
        )
        
        if final_df.empty:
            upload_log_file_to_s3(log_file_path, log_file_bucket_name, log_file_key)
            raise RuntimeError("Merged dataframe is empty.")
        
    except Exception as e:
        logging.error(f"An error occurred during data merging: {e}")
        upload_log_file_to_s3(log_file_path, log_file_bucket_name, log_file_key)
        raise RuntimeError("Failed to Merge Datasets.") from e

    # Load currency data from S3 and integrate it into final_df
    s3_currency = s3fs.S3FileSystem()
    with s3_currency.open(f'{currency_bucket_name}/{currency_file_key}', 'rb') as f:
        currency_df = pd.read_csv(f, compression='gzip')

    month_end_currency_data = get_last_reporting_start_date_rows(currency_df)
    
    # Map conversion rates
    final_df = map_conversion_rates(month_end_currency_data, final_df)
    
    # Map revenue and cost in USD
    final_df = map_revenue_cost_usd(final_df)
    
    final_df = final_df.reindex(columns=[
        'VENDOR_NAME', 'VENDOR_ASSET_ID', 'TERRITORY', 'TRANSACTION_DATE', 'TITLE', 'PRIMARY_GENRE', 
        'TRANSACTION_FORMAT', 'MEDIA_FORMAT', 'RETAIL_PRICE_NATIVE', 'UNIT_COST_NATIVE', 'QUANTITY', 
        'REVENUE_NATIVE', 'COST_NATIVE', 'REVENUE_USD', 'COST_USD', 'CONVERSION_RATE'])

    # Write Transformed Data to S3 with partitions
    try:
        write_data_to_s3(final_df, output_bucket_name, output_folder_key)
    except Exception as e:
        logging.error(f"An error occurred during data writing: {e}")
        upload_log_file_to_s3(log_file_path, log_file_bucket_name, log_file_key)
        raise RuntimeError("Failed to Write Data.") from e
    
    # Write metadata
    # Get metadata from current iteration
    new_raw_metadata_df = raw_metadata(new_raw_metadata_amazon, new_raw_metadata_itunes, new_raw_metadata_google)
    
    # Get metadata after completion of data transformation
    new_processed_metadata_all = pd.DataFrame(new_processed_metadata_all)
    
    # Concat new_raw_metadata_df and new_processed_metadata_all
    combined_processed_metadata = concat_metadata(new_raw_metadata_df, new_processed_metadata_all)
    
    combined_processed_metadata_filtered = combined_processed_metadata.dropna(how='any')
    
    # Get all files with null metadata
    files_not_processed = pd.concat([combined_processed_metadata, combined_processed_metadata_filtered]).drop_duplicates(keep=False)
    file_names = files_not_processed['raw_file_path'].tolist()
    logging.info(f'List of all files which have null metadata: {file_names}')
    
    # Put metadata file in s3
    append_metadata_to_csv(combined_processed_metadata_filtered, processed_metadata_bucket, processed_metadata_file_key)
    
    # Raw metrics data
    raw_metrics_data = raw_metadata(metric_metadata_amazon, metric_metadata_itunes, metric_metadata_google)
    
    # create df of processed_metric
    processed_metrics_data = pd.DataFrame(metric_metadata)
    
    # combine metrics metadata
    matrics_metadata = concat_metadata(raw_metrics_data, processed_metrics_data)
    
    matrics_metadata = matrics_metadata.explode(['metric', 'raw_file_value', 'processed_file_value'])
    matrics_metadata_filtered = matrics_metadata.dropna(how='any')
    
    # append metric metadata to s3
    append_metadata_to_csv(matrics_metadata_filtered, metric_bucket_name, metric_file_key)
    
except Exception as e:
    logging.error(f"An error occurred in the main script: {e}")
    upload_log_file_to_s3(log_file_path, log_file_bucket_name, log_file_key)
    raise e
    
upload_log_file_to_s3(log_file_path, log_file_bucket_name, log_file_key)


# In[ ]:




