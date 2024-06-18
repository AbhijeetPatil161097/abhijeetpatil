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

s3 = s3fs.S3FileSystem()

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
new_raw_metadata = []

new_processed_metadata_all = []

metric_metadata = []

metric_metadata_processed = []


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
        script = s3.cat(f"s3://{bucket_name}/{file_key}").decode('utf-8')
        return script
    except Exception as e:
        logging.error(f"An error occurred while reading script from S3: {e}")
        raise RuntimeError("Failed to run function read_script_from_s3.") from e
        
        
def _filter_partner_files(files_to_process, partner):
    filtered_df =  files_to_process[files_to_process['partner'] == partner]
    return filtered_df

def _read_file_from_s3(bucket_name, file_key, file_extension):
    try:
        file_path = f"s3://{bucket_name}/{file_key}"
        
        if file_extension == '.csv':
            return pd.read_csv(file_path)
        elif file_extension == '.tsv':
            return pd.read_csv(file_path, sep='\t')
        elif file_extension == '.xlsx':
            return pd.read_excel(file_path)
        elif file_extension == '.gz':
            with s3.open(file_path, 'rb') as f:
                return pd.read_csv(f, sep='\t', compression='gzip')
        else:
            raise ValueError(f"Unsupported file format: {file_path}{file_extension}")
    except Exception as e:
        logging.error(f"An error occurred while reading file from S3: {e}")
        return None

    
def _collect_file_metadata(bucket_name, 
                           file_key, 
                           file_name, 
                           file_creation_date,
                           file_name_month,
                           partner,
                           unique_months,
                           file_row_count
                          ):
    try:
        new_raw_metadata.append({
            'raw_file_path': f"s3://{bucket_name}/{file_key}", 
            'raw_file_name': file_name,                       
            'raw_file_creation_date': file_creation_date,
            'raw_file_name_month': file_name_month,
            'platform': 'DTO',
            'partner': partner,
            'months_in_data': unique_months,
            'raw_file_row_count': file_row_count
        })
    except Exception as e:
        logging.error(f"Failed to append metadata in list new_raw_metadata_amazon: {file_key}, Error: {e}")
        

def _collect_metric_metadata(bucket_name,
                             file_key, 
                             file_name, 
                             partner,
                             unique_months,
                             metrics=None, 
                             raw_file_values=None
                            ):
    """Collects metadata for metric validation."""
    try:
        metadata = {
            'raw_file_path': f"s3://{bucket_name}/{file_key}", 
            'raw_file_name': file_name,                       
            'platform': 'DTO',
            'partner': partner,
            'months_in_data': unique_months,
        }
        
        if metrics is not None:
            metadata['metric'] = metrics

        if raw_file_values is not None:
            metadata['raw_file_value'] = raw_file_values
        
        metric_metadata.append(metadata)
    except Exception as e:
        logging.error(f"Failed to append metadata in list metric_metadata: {file_key}, Error: {e}")
        
        
def _collect_processed_metadata(bucket_name, 
                           file_key, 
                           file_name, 
                           file_creation_date,
                           file_name_month,
                           partner,
                           unique_months,
                           file_row_count
                          ):
    try:
        new_raw_metadata.append({
            'raw_file_path': f"s3://{bucket_name}/{file_key}", 
            'raw_file_name': file_name,                       
            'raw_file_creation_date': file_creation_date,
            'raw_file_name_month': file_name_month,
            'platform': 'DTO',
            'partner': partner,
            'months_in_data': unique_months,
            'raw_file_row_count': file_row_count
        })
    except Exception as e:
        logging.error(f"Failed to append metadata in list new_raw_metadata_amazon: {file_key}, Error: {e}")
        
        

        
def _remove_associated_files(partner_df, new_files_amazon, new_raw_metadata, partner):
    try:
        files_with_issue = new_files_amazon.merge(new_raw_metadata, 
                                                  how='left',
                                                  on=['raw_file_path', 'months_in_data'],
                                                  indicator=True)
        files_with_issue = files_with_issue[files_with_issue['_merge'] == 'left_only']
        files_with_issue.drop('_merge', axis=1, inplace=True)
        months_in_data = files_with_issue['months_in_data'].tolist()
        logging.error(f"Files not processed for Amazon date: {files_with_issue['months_in_data'].unique().tolist()}")
        
        if partner == 'amazon':
            return partner_df[~partner_df['TRANSACTION_DATE'].isin(months_in_data)]
        elif partner == 'itunes':
            return partner_df[~partner_df['Begin Date'].isin(months_in_data)]
        elif partner == 'google':
            return partner_df[~partner_df['Transaction Date'].isin(months_in_data)]
            
    except Exception as e:
        logging.error(f"Failed to remove faulty files from Amazon data, Error: {e}")


def _extract_date_from_file_key(file_key, partner):
    if partner == 'amazon':
        return '-'.join(file_key.split('_')[-1].split('-')[:2])
    
    if partner == 'itunes':
        if file_key.endswith('.gz'):
            date_match = re.search(r'(\d{8})\.txt\.gz$|(\d{8})\.gz$', file_key)
        else:
            date_match = re.search(r'_(\d{8})|_(\d{2})(\d{2})_', file_key)
        
        return (datetime.strptime(date_match.group(1) or f"20{date_match.group(3)}-{date_match.group(2)}", '%Y%m%d')
                if date_match else None).strftime('%Y-%m')

    if partner == 'google':
        return datetime.strptime(re.search(r'(\d{8})', file_key).group(1), '%Y%m%d').strftime('%Y-%m')        
        

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
        for vendor_name, group in df.groupby('PARTNER'):
            for (year, month), data in group.groupby([
                pd.to_datetime(group['TRANSACTION_DATE']).dt.year,
                pd.to_datetime(group['TRANSACTION_DATE']).dt.month
            ]):
                # Define file name and key
                file_name = f"vendor_name={vendor_name}/year={year}/{year}-{month}.csv" 
                file_key_name = f"{file_key}/{file_name}"
                
                # Collect metadata for this file
                transaction_date = f"{year}-{month:02d}"
                new_processed_metadata_all.append({
                    'processed_file_row_count': len(data),
                    'processed_date': datetime.now().strftime('%Y-%m-%d'),
                    'processed_file_path': f"s3://{bucket_name}/{file_key_name}",
                    'months_in_data': transaction_date,
                    'partner': vendor_name
                })
                try:
                    if vendor_name == 'amazon':
                        metric_metadata_processed.append({
                            'processed_file_value': [data['QUANTITY'].sum(), 
                                                     data['REVENUE_NATIVE'].sum()
                                                    ],
                            'processed_date': datetime.now().strftime('%Y-%m-%d'),
                            'months_in_data': transaction_date,
                            'partner': vendor_name
                        })
                    elif vendor_name == 'itunes':
                        try:
                            metric_metadata_processed.append({
                                'processed_file_value': [data['QUANTITY'].sum(),
                                                         data['REVENUE_NATIVE'].sum()
                                                        ],
                                'processed_date': datetime.now().strftime('%Y-%m-%d'),
                                'months_in_data': transaction_date,
                                'partner': vendor_name,
                            })
                        except:
                            logging.error(f"An error occurred while writing itunes metric data to S3: {e}")
                    elif vendor_name == 'google':
                        metric_metadata_processed.append({
                            'processed_file_value': [data['QUANTITY'].sum(),
                                                     data['REVENUE_NATIVE'].sum()
                                                    ],
                            'processed_date': datetime.now().strftime('%Y-%m-%d'),
                            'months_in_data': transaction_date,
                            'partner': vendor_name
                        })

                except Exception as e:
                    logging.error(f"An error occurred while writing metric data to S3: {e}")
                    raise RuntimeError("Failed to write data to S3.") from e
                path = f's3://{bucket_name}/{file_key_name}'
                data.to_csv(path, index=False, mode='w')
                logging.info(f"Data writing to S3 is successful for file{file_name}.")
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
    new_processed_metadata = raw_metadata.merge(processed_metadata, on=['months_in_data', 'partner'], how='left')
    
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
    Maps conversion rates to transformed data (Amazon, iTunes, Google).
    
    Parameters:
        month_end_currency_data: DataFrame with month-end currency data.
        final_df: DataFrame with transformed partner data.
    
    Returns:
        DataFrame with updated conversion rates.
    """
    try:
        # Create a conversion map from the month-end data
        conversion_map = {
            (date, country): rate
            for date, country, rate in zip(
                month_end_currency_data['REPORTING_START_DATE'], 
                month_end_currency_data['COUNTRY_CODE'], 
                month_end_currency_data['CONVERSION_RATE']
            )
        }
    
        # Apply conversion rates to rows where IS_CONVERSION_RATE is False
        final_df.loc[~final_df['IS_CONVERSION_RATE'], 'CONVERSION_RATE'] = final_df[~final_df['IS_CONVERSION_RATE']].apply(
            lambda row: conversion_map.get(
                (row['TRANSACTION_DATE'], 'GB') if row['TERRITORY'] == 'UK' else (row['TRANSACTION_DATE'], row['TERRITORY']), None), axis=1)
        logging.info("Conversion rates mapping is successful.")
        return final_df
        logging.error(f"{final_df['CONVERSION_RATE'].tolist()}")

    except Exception as e:
        logging.error(f"An error occurred while mapping conversion rates: {e}")
        upload_log_file_to_s3(log_file_path, log_file_bucket_name, log_file_key)
        raise RuntimeError("Failed to map conversion rates.") from e

        
        
# Map values of revenue_usd and cost_usd to dataframe
def map_revenue_cost_usd(df):
    """
    Maps revenue and cost in USD in the DataFrame.
    
    Parameters:
        df: DataFrame from the output of function (map_conversion_rates) after mapping conversion rates.
    
    Returns:
        DataFrame after mapping revenue and cost in USD.
    """
    try:
        # Apply conversion only if IS_CONVERSION_RATE is False
        if not df['IS_CONVERSION_RATE'].all():
            df.loc[~df['IS_CONVERSION_RATE'], 'REVENUE_USD'] = df['REVENUE_NATIVE'] * df['CONVERSION_RATE']
            df.loc[~df['IS_CONVERSION_RATE'], 'RETAIL_PRICE_USD'] = df['RETAIL_PRICE_NATIVE'] * df['CONVERSION_RATE']
            df.loc[~df['IS_CONVERSION_RATE'], 'UNIT_REVENUE_USD'] = df['UNIT_REVENUE_NATIVE'] * df['CONVERSION_RATE']
            df.loc[~df['IS_CONVERSION_RATE'], 'UNIT_RETAIL_PRICE_USD'] = df['UNIT_RETAIL_PRICE_NATIVE'] * df['CONVERSION_RATE']
        
            logging.info("Revenue and Retail Price in USD mapping is successful.")
            return df
        else:
            logging.info("Revenue and Retail Price in USD not mapped.")
            return df
        
    except Exception as e:
        logging.error(f"An error occurred while mapping revenue and cost in USD: {e}")
        upload_log_file_to_s3(log_file_path, log_file_bucket_name, log_file_key)
        raise RuntimeError("Failed to map revenue and cost in USD.") from e

        

def raw_metadata(raw_metadata):
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
    metadata_df = pd.DataFrame(raw_metadata)
    return metadata_df


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
        logging.info(f"Dataframes merged successfully")
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
    logging.error(f"DataFrame content:\n{month_end_currency_data.head().to_string()}")
    # Map conversion rates
    final_df = map_conversion_rates(month_end_currency_data, final_df)
    logging.error(f"DataFrame content:\n{final_df.head().to_string()}")
    # Map revenue and cost in USD
    final_df = map_revenue_cost_usd(final_df)
    logging.error(f"DataFrame content:\n{final_df.head().to_string()}")
    final_df = final_df.reindex(columns=[
        'PARTNER', 'VENDOR_ASSET_ID', 'TERRITORY', 'TRANSACTION_DATE', 'PARTNER_TITLE', 'TITLE', 
        'TRANSACTION_FORMAT', 'MEDIA_FORMAT', 'TRANSACTION_TYPE', 'PURCHASE_LOCATION', 'VIDEO_CATEGORY', 
        'CYS_EPISODE_COUNT', 'QUANTITY', 'UNIT_RETAIL_PRICE_NATIVE', 'UNIT_RETAIL_PRICE_USD', 'RETAIL_PRICE_NATIVE', 
        'RETAIL_PRICE_USD', 'UNIT_REVENUE_NATIVE', 'UNIT_REVENUE_USD', 'REVENUE_NATIVE', 'REVENUE_USD', 'CONVERSION_RATE',
        'IS_CONVERSION_RATE'
    ])

    # Write Transformed Data to S3 with partitions
    try:
        write_data_to_s3(final_df, output_bucket_name, output_folder_key)
    except Exception as e:
        logging.error(f"An error occurred during data writing: {e}")
        upload_log_file_to_s3(log_file_path, log_file_bucket_name, log_file_key)
        raise RuntimeError("Failed to Write Data.") from e
    
    
    # Get metadata from current iteration
    new_raw_metadata_df = raw_metadata(new_raw_metadata)
    
    # Get metadata after completion of data transformation
    new_processed_metadata_all = pd.DataFrame(new_processed_metadata_all)
    
    # Concat new_raw_metadata_df and new_processed_metadata_all
    combined_processed_metadata = concat_metadata(new_raw_metadata_df, new_processed_metadata_all)
    
    # Drop rows with any null values
    combined_processed_metadata_filtered = combined_processed_metadata.dropna(how='any')
    
    # Get all files with null metadata
    files_not_processed = pd.concat([combined_processed_metadata, combined_processed_metadata_filtered]) \
                            .drop_duplicates(keep=False)
    
    # Log file names to log file
    file_names = files_not_processed['raw_file_path'].tolist()
    logging.info(f'List of all files which have null metadata: {file_names}')
    

    # Put metadata file in s3
    append_metadata_to_csv(combined_processed_metadata_filtered, processed_metadata_bucket, processed_metadata_file_key)
    
    # Raw metrics data
    raw_metrics_data = raw_metadata(metric_metadata)
    logging.error(f"DataFrame content:\n{raw_metrics_data.to_string()}")
    # create df of processed_metric
    processed_metrics_data = pd.DataFrame(metric_metadata_processed)
    logging.error(f"DataFrame content:\n{processed_metrics_data.to_string()}")
    
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
