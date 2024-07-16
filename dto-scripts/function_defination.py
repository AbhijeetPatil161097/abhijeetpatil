# log file related functions

# Initialize logging (for DTO_Script_1 & DTO_Script_2)
def initialize_logging(log_file_path):
    """
    Function:
        * Initializes logging file.
    
    Parameters:
        * log_file_path : Path of log file
    
    """
    try:
        logging.basicConfig(level=logging.INFO, 
                            format='%(asctime)s - %(levelname)s - %(message)s', 
                            filename=log_file_path
                           )
        return True
    except Exception as e:
        print(f"An error occurred while initializing logging: {e}")
        return False
    


# Upload log file to s3 (for DTO_Script_1 & DTO_Script_2)
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

#-------------------------------------------------------------------------------------------------------------------------

# Metadata related functions

# Collects metadata of raw files (for DTO_Script_1)
def _collect_file_metadata(bucket_name, 
                           file_key, 
                           file_name, 
                           file_creation_date,
                           file_name_month,
                           partner,
                           unique_months,
                           file_row_count
                          ):
    """
    Function:
        * Collects Metadata of file and appends it in list new_raw_metadata.
    
    """
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
        


# Collects raw files metric metadata (for DTO_Script_2)
def _collect_metric_metadata(bucket_name,
                             file_key, 
                             file_name, 
                             partner,
                             unique_months,
                             metrics=None, 
                             raw_file_values=None
                            ):
    """
    Function:
        * Collects metadata for metric validation and appends in list metric_metadata.
    
    """
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
        

        
# Collects metadata of processed files (for DTO_Script_2)
def _collect_processed_metadata(data,
                                bucket_name,
                                file_key_name,
                                transaction_date,
                                partner
                                ):
    """
        Function:
            * Collects Metadata of processed file and appends it in list new_raw_metadata.
    
    """
    try:
        new_processed_metadata.append({
            'processed_file_row_count': len(data),
            'processed_date': datetime.now().strftime('%Y-%m-%d'),
            'processed_file_path': f"s3://{bucket_name}/{file_key_name}",
            'months_in_data': transaction_date,
            'partner': partner
        })
    except Exception as e:
        logging.error(f"Failed to append metadata in list new_processed_metadata: {file_key}, Error: {e}")



# Collects processed metric metadata (for DTO_Script_2)
def _collect_processed_metric_metadata(data, transaction_date, partner):
    """
    Collects processed metric metadata and appends it to metric_metadata_processed list.
    
    Parameters:
        * data: DataFrame containing processed data.
        * transaction_date: Transaction date in 'YYYY-MM' format.
        * partner: Partner name (e.g., 'amazon', 'itunes', 'google').
    """
    try:
        metric_metadata_processed.append({
            'processed_file_value': [data['QUANTITY'].sum(), data['REVENUE_NATIVE'].sum()],
            'processed_date': datetime.now().strftime('%Y-%m-%d'),
            'months_in_data': transaction_date,
            'partner': partner
        })
    except Exception as e:
        logging.error(f"An error occurred while collecting metric metadata for partner {partner}: {e}")
        raise RuntimeError(f"Failed to collect metric metadata for partner {partner}.") from e
    


# Function to read existing raw metadata (for DTO_Script_1)
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
    try:
        s3_url = f"s3://{bucket_name}/{file_key}"
        files_to_process = pd.read_csv(s3_url)
        return files_to_process
    except Exception as e:
        logging.error(f"An error occurred in function read_new_files: {e}")
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
    


# concat processed metadata (for DTO_Script_1 & DTO_Script_2)
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



# Append metadata file to s3 (for DTO_Script_1 & DTO_Script_2)
def append_metadata_to_csv(df, bucket_name, file_key):
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

    def file_exists():
        """
        Check if the file exists in the S3 bucket.
        
        """
        return s3.exists(path)

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
            reordered_metadata = reorder_columns(df, existing_columns)
            append_data(reordered_metadata)
        else:
            # If File does not exist, write new data
            write_data(df)
    except Exception as e:
        logging.error(f"An error occurred while uploading metadata: {e}")


        
# Log non processed files (for DTO_Script_2)
def process_and_append_processed_metadata(new_raw_metadata, new_processed_metadata, metadata_bucket, processed_metadata_file_key):
    """
    Function:
        * Log file names of files not properly processed.
    Parameters:
        * new_raw_metadata: Metadata of raw files.
        * new_processed_metadata: Metadata of processed files
    Returns:
        Filtered metadata Dataframe.
    """
    # Get metadata from current iteration
    def create_raw_metadata_df(new_raw_metadata):
        '''Create dataframe'''
        raw_metadata_df = create_dataframe(new_raw_metadata)
        return raw_metadata_df
    
    # Get metadata after completion of data transformation
    def create_processed_metadata_df(new_processed_metadata):
        '''Craete dataframe'''
        processed_metadata_df = create_dataframe(new_processed_metadata)
        return processed_metadata_df
    
    # Concatenate new_raw_metadata_df and new_processed_metadata
    def concat_and_filter(raw_metadata_df, processed_metadata_df):
        '''Concat dataframes on 'partner and month_in_data' '''
        combined_processed_metadata = concat_metadata(raw_metadata_df, processed_metadata_df)
        
        # Drop rows with any null values
        combined_processed_metadata_filtered = combined_processed_metadata.dropna(how='any')
        
        # Get all files with null metadata
        files_not_processed = pd.concat([combined_processed_metadata, combined_processed_metadata_filtered]) \
                                .drop_duplicates(keep=False)
        
        # Log file names to log file
        file_names = files_not_processed['raw_file_path'].tolist()
        logging.info(f'List of all files which have null metadata: {file_names}')
        return combined_processed_metadata_filtered, file_names
    
    raw_metadata_df = create_raw_metadata_df(new_raw_metadata)
    processed_metadata_df = create_processed_metadata_df(new_processed_metadata)
    combined_processed_metadata_filtered, file_names = concat_and_filter(raw_metadata_df, processed_metadata_df)

    # Log dropped rows
    if file_names:
        logging.info(f"Dropped rows due to null values:\n{file_names}")

    # Append metric metadata to S3
    append_metadata_to_csv(combined_processed_metadata_filtered, metadata_bucket, processed_metadata_file_key)

    # Log success message
    logging.info(f"Processed files metadata successfully appended to S3 bucket: {metadata_bucket}/{processed_metadata_file_key}")



# Append metrics file to s3 (for DTO_Script_2)
def process_and_append_metrics_metadata(metric_metadata, metric_metadata_processed, metadata_bucket, metric_file_key):
    """
    Function:
        * Filters and appends metric metadata to s3.
    Parameters:
        * metric_metadata: Metric metadata of raw files.
        * metric_metadata_processed : Metric metadata of processed files.
        * metric_bucket_name: File bucket name.
        * metric_file_key: File prefix key.

    """
    # Raw metrics data
    def raw_metrics_df(metric_metadata):
        '''Create Dataframe'''
        raw_metrics_data = create_dataframe(metric_metadata)
        return raw_metrics_data
    
    # Create DataFrame of processed metrics
    def processed_metrics_df(metric_metadata_processed):
        '''Create Dataframe'''
        processed_metrics_data = create_dataframe(metric_metadata_processed)
        return processed_metrics_data
    
    # Combine metrics metadata and explode contents
    def concat_and_explode(raw_metrics_data, processed_metrics_data):
        '''Concat dataframes on 'partner and month_in_data' and explode rows. '''
        metrics_metadata = concat_metadata(raw_metrics_data, processed_metrics_data)

        # explode combine rows into multiple rows
        metrics_metadata = metrics_metadata.explode(['metric', 'raw_file_value', 'processed_file_value'])

        # Drop rows with any null values
        metrics_metadata_filtered = metrics_metadata.dropna(how='any')

        # Filter rows with null values
        dropped_rows = metrics_metadata[~metrics_metadata.index.isin(metrics_metadata_filtered.index)]
        return metrics_metadata_filtered, dropped_rows

    raw_metrics_data = raw_metrics_df(metric_metadata)
    processed_metrics_data = processed_metrics_df(metric_metadata_processed)
    metrics_metadata_filtered, dropped_rows = concat_and_explode(raw_metrics_data, processed_metrics_data)

    # Log dropped rows
    if not dropped_rows.empty:
        logging.info(f"Dropped rows due to null values:\n{dropped_rows}")
    
    # Append metric metadata to S3
    append_metadata_to_csv(metrics_metadata_filtered, metadata_bucket, metric_file_key)

    # Log success message
    logging.info(f"Metrics metadata successfully appended to S3 bucket: {metric_bucket_name}/{metric_file_key}")

#-------------------------------------------------------------------------------------------------------------------------
# Currency related functions

# Get currency data (for DTO_Script_2)
def get_currency_df(currency_bucket_name, currency_file_key):
    '''
    Function:
        * Get currency data from s3 and create dataframe

    Returns:
        * Dataframe of currency data    
    '''
    with s3.open(f'{currency_bucket_name}/{currency_file_key}', 'rb') as f:
        currency_df = pd.read_csv(f, compression='gzip')
        return currency_df



# Get currency data of last date of month of each currency (for DTO_Script_2)
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
        result_df['REPORTING_START_DATE'] = result_df['REPORTING_START_DATE'].dt.strftime('%Y-%m')
        logging.info(f"Detching Conversion Rates for month end dates successful")
        return result_df
    except Exception as e:
        logging.error(f"An error occurred while getting last reporting start date rows: {e}")



# Map conversion rates to dataframe (for DTO_Script_2)
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
        conversion_map = {(date, country): conversion_rate for date, country, conversion_rate in zip(
            month_end_currency_data['REPORTING_START_DATE'], 
            month_end_currency_data['COUNTRY_CODE'], 
            month_end_currency_data['CONVERSION_RATE']
        )}
        # Apply conversion rates only where IS_CONVERSION_RATE is False
        def get_conversion_rate(row):
            if row['IS_CONVERSION_RATE'] == False:
                # Map GB conversion rates for UK Territory.
                key = (row['TRANSACTION_DATE'], 'GB') if row['TERRITORY'] == 'UK' else (row['TRANSACTION_DATE'], row['TERRITORY'])
                return conversion_map.get(key, None)
            return row['CONVERSION_RATE']
        
        final_df['CONVERSION_RATE'] = final_df.apply(get_conversion_rate, axis=1)
        
        logging.info("Conversion rates mapping is successful.")
        return final_df

    except Exception as e:
        logging.error(f"An error occurred while mapping conversion rates: {e}")
        upload_log_file_to_s3(log_file_path, log_file_bucket_name, log_file_key)
        raise RuntimeError("Failed to map conversion rates.") from e
    


# Map values of revenue_usd and cost_usd to dataframe (for DTO_Script_2)
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
        mask = df['IS_CONVERSION_RATE'] == False
        
        if mask.any():
            df.loc[mask, 'REVENUE_USD'] = df.loc[mask, 'REVENUE_NATIVE'] * df.loc[mask, 'CONVERSION_RATE']
            df.loc[mask, 'RETAIL_PRICE_USD'] = df.loc[mask, 'RETAIL_PRICE_NATIVE'] * df.loc[mask, 'CONVERSION_RATE']
            df.loc[mask, 'UNIT_REVENUE_USD'] = df.loc[mask, 'UNIT_REVENUE_NATIVE'] * df.loc[mask, 'CONVERSION_RATE']
            df.loc[mask, 'UNIT_RETAIL_PRICE_USD'] = df.loc[mask, 'UNIT_RETAIL_PRICE_NATIVE'] * df.loc[mask, 'CONVERSION_RATE']
        
            logging.info("Revenue and Retail Price in USD mapping is successful.")
        else:
            logging.info("No conversion needed as all rows have IS_CONVERSION_RATE set to True.")
        return df
    except Exception as e:
        logging.error(f"An error occurred while mapping revenue and cost in USD: {e}")
        upload_log_file_to_s3(log_file_path, log_file_bucket_name, log_file_key)
        raise RuntimeError("Failed to map revenue and cost in USD.") from e
    

#-------------------------------------------------------------------------------------------------------------------------
# General Functions

# Create raw metadata dataframe (for DTO_Script_1 & DTO_Script_2)
def create_dataframe(raw_metadata):
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



# Get new files to process (for DTO_Script_1)
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
        Gets only unique combinations of partner and months_in_data.
        
        * filter_matching_rows:
        Get all rows from exploded current_metadata_df associated with unique partner and months_in_data
    
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
                   based on ('raw_file_path', 'raw_file_creation_date', 'partner', 'months_in_data')
                   
        """
        on_cols = ['raw_file_path', 'raw_file_creation_date', 'partner', 'months_in_data']
        
        merged_df = current_df.merge(old_df, on=on_cols, how='left', indicator=True)
        
        merged_df = merged_df.query('_merge=="left_only"')[['months_in_data', 'partner']].drop_duplicates()
        return merged_df

    def filter_matching_rows(current_metadata_df, merged_df):
        """
        Gets all rows with unique combinations of partner and months_in_data from merged_df
        
        Params:
            * current_metadata_df: Exploded dataframe from explode_months_in_data_column
            * merged_df: Dataframe of unique combination of months_in_data and partner.
            
        Returns:
        matching_rows_df: Dataframe of all matching rows matching combination of months_in_data and partner
                          from current_metadata_df
        """
        matching_rows_df = pd.merge(current_metadata_df, merged_df, on=['months_in_data', 'partner'], how='inner')
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
            return pd.DataFrame()



# Upload new files df to S3 (for DTO_Script_1)
def upload_new_files_to_csv(new_files_df, bucket_name, file_key):
    """
    Function:
        * Uploads new files DataFrame to a CSV file in an S3 bucket.

    Parameters:
        * new_files_df: DataFrame containing new files to be processed.
        * bucket_name: The name of the S3 bucket.
        * file_key: The S3 key (file path) for the metadata CSV file.
    
    """
    try:
        # Upload new files DataFrame to new_files_key
        with s3.open(f"{bucket_name}/{file_key}", 'wb') as f:
            new_files_df.to_csv(f, index=False)
        
    except Exception as e:
        logging.error(f"An error occurred while uploading new files to CSV: {e}")
        raise
    


# Read new files to process (for DTO_Script_2)
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

    

# Read script from S3 (for DTO_Script_2)
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
    


# Filters dataframe for specific partner (for DTO_Script_2)
def _filter_partner_files(files_to_process, partner):
    """
    Function:
        * Filters files_to_process dataframe based on partner name.
    
    Parameters:
        * files_to_process: Dataframe of files to process.
        
    Returns:
        * Filtered DataFrame based on partner name.
    """
    filtered_df =  files_to_process[files_to_process['partner'] == partner]
    return filtered_df



# Reads file with different extensions (for DTO_Script_2)
def _read_file_from_s3(bucket_name, file_key, file_extension):
    """
    Function:
        * Read files with different file extension.
    
    Parameters:
        * bucket_name: Bucket name of input data.
        * File Key: File key prefix for file.
        * file_extension: extension of given file.
        
    Returns:
        * Dataframe of raw file data.
    
    """
    try:
        file_path = f"s3://{bucket_name}/{file_key}"
        
        if file_extension == '.csv':
            return pd.read_csv(file_path)
        elif file_extension == '.tsv':
            return pd.read_csv(file_path, sep='\t')
        elif file_extension == '.xls':
            return pd.read_csv(file_path, sep='\t')
        elif file_extension == '.xlsx':
            return pd.read_excel(file_path)
        elif file_extension == '.gz':
            with s3.open(file_path, 'rb') as f:
                return pd.read_csv(f, sep='\t', compression='gzip')
        else:
            logging.error(f"Unsupported file format: {file_path}{file_extension}")
            return None
    except Exception as e:
        logging.error(f"An error occurred while reading file from S3: {e}")



# Removes files from dataframe (for DTO_Script_2)
def _remove_associated_files(partner_df, new_files_df, new_raw_metadata, partner):
    """
    Function:
        * Removes all data asssociated with same partner and month of any unprocessed file to prevent 
        processing incomplete data
        
    Parameters:
        * partner_df : DataFrame of partner raw files.
        * new_files_df: filtered DataFrame of files to process on partner.
        * new_raw_metadata : DataFrame of metadata of current iteration raw files.
        * parrtner: name of partner in string.
    
    Returns:
        * DataFrame after removing all associated files.
    """
    try:
        files_with_issue = new_files_df.merge(new_raw_metadata, 
                                                  how='left',
                                                  on=['raw_file_path', 'months_in_data'],
                                                  indicator=True)
        files_with_issue = files_with_issue[files_with_issue['_merge'] == 'left_only']
        files_with_issue.drop('_merge', axis=1, inplace=True)
        months_in_data = files_with_issue['months_in_data'].tolist()
        logging.error(f"Files not processed for {partner} date: {files_with_issue['months_in_data'].unique().tolist()}")
        
        if partner == 'amazon':
            return partner_df[~partner_df['TRANSACTION_DATE'].isin(months_in_data)]
        elif partner == 'itunes':
            return partner_df[~partner_df['Start Date'].isin(months_in_data)]
        elif partner == 'google':
            return partner_df[~partner_df['Transaction Date'].isin(months_in_data)]
            
    except Exception as e:
        logging.error(f"Failed to remove faulty files from Amazon data, Error: {e}")



# Extract date from file name. (for DTO_Script_1 & DTO_Script_2)
def _extract_date_from_file_key(file_key, partner):
    """
    Function:
        * Extracts date from file key.
        
    Returns:
        * Date in yyyy-mm format.
    
    """
    if partner == 'amazon':
        return '-'.join(file_key.split('_')[-1].split('-')[:2])
    
    if partner == 'itunes':
        if file_key.endswith('.gz'):
            date_match = re.search(r'(\d{8})\.txt\.gz$|(\d{8})\.gz$', file_key)
        else:
            date_match = re.search(r'_(\d{8})|_(\d{2})(\d{2})_', file_key)
        
        return (datetime.strptime(date_match.group(1) or f"20{date_match.group(3)}-{date_match.group(2)}", '%Y-%m')
                if date_match else None).strftime('%Y-%m')

    if partner == 'google':
        return datetime.strptime(re.search(r'(\d{8})', file_key).group(1), '%Y%m%d').strftime('%Y-%m')     
    

# Reindex columns
def reindex_dataframe(df):
    """
    Function:
        * Reindexes the given DataFrame with specific columns.

    Parameters:
        * df : The DataFrame to be reindexed.

    Returns:
        * Reindexed dataframe
    """
    columns = [
        'PARTNER', 'VENDOR_ASSET_ID', 'TERRITORY', 'TRANSACTION_DATE', 'PARTNER_TITLE', 'TITLE', 
        'TRANSACTION_FORMAT', 'MEDIA_FORMAT', 'TRANSACTION_TYPE', 'PURCHASE_LOCATION', 'VIDEO_CATEGORY', 
        'CYS_EPISODE_COUNT', 'QUANTITY', 'UNIT_RETAIL_PRICE_NATIVE', 'UNIT_RETAIL_PRICE_USD', 
        'RETAIL_PRICE_NATIVE', 'RETAIL_PRICE_USD', 'UNIT_REVENUE_NATIVE', 'UNIT_REVENUE_USD', 
        'REVENUE_NATIVE', 'REVENUE_USD', 'CONVERSION_RATE', 'IS_CONVERSION_RATE'
    ]
    
    # Reindex the DataFrame with the specified columns
    reindexed_df = df.reindex(columns=columns)
    
    return reindexed_df

#-------------------------------------------------------------------------------------------------------------------------

# Data reading, processing and writing related functions

# Read amazon data (for DTO_Script_1)
def read_and_append_amazon_metadata(bucket_name, prefix):
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
            except Exception as e:
                logging.error(f"Error reading amazon file:{file_key}, {e}")
                continue
    except Exception as e:
       logging.error(f"An error occurred while reading data from S3 Amazon: {e}")
       upload_log_file_to_s3(log_file_path, log_file_bucket_name, log_file_key)



# Read itunes data (for DTO_Script_1)
def read_and_append_itunes_metadata(bucket_name, prefix):
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
                logging.info(f"bucket_name = {bucket_name}")
                file_key = file_path.split(f'{bucket_name}/')[1]

                logging.info(f"File key, {file_key}")
                file_name = os.path.basename(file_key)
                logging.info(f"Processing started for file: {file_key}")
                
                # Get file creation date using s3fs
                file_info = s3.info(f"{bucket_name}/{file_key}")
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
                    
                    df['FILE_NAME_DATE'] = file_name_month

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
      
    

# Read google data (for DTO_Script_1)
def read_and_append_google_metadata(bucket_name, prefix):
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
               file_info = s3.info(f"{bucket_name}/{file_key}")
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
               
               df['FILE_NAME_DATE'] = file_name_month
               
               if (df['Transaction Date'] == df['FILE_NAME_DATE']).all():
                   continue
               else:
                   logging.error(f"Transaction Date does not match FILE_NAME_DATE for: {file_key}")
            except Exception as e:
              logging.error(f"Error reading google file:{file_key}, {e}")
              continue 
    except Exception as e:
       logging.error(f"An error occurred while reading data from S3 Google: {e}")
       upload_log_file_to_s3(log_file_path, log_file_bucket_name, log_file_key)
       


# Read and process amazon data (for DTO_Script_2)
def process_amazon_data(files_to_process, input_bucket_name):
    '''
    Function:
        * Reads amazon data from s3
        * Processes amazon data from S3
    
    Parameters:
        * files_to_process: dataframe of files to process (updated in DTO_Script_1 execution)
        * input_bucket_name: Bucket name of raw data files.

    Returns:
        * Dataframe of processed amazon data.
    '''
    try:
        df_amazon = read_data_from_s3_amazon(files_to_process, input_bucket_name)
        amazon_monthly_data = DtoDataProcessAmazon('Amazon', df_amazon)
        return amazon_monthly_data.process_data_source()
    except Exception as e:
        logging.error(f"An error occurred during Amazon data processing: {e}")
        return pd.DataFrame()
    


# Read and process itunes data (for DTO_Script_2)
def process_itunes_data(files_to_process, input_bucket_name):
    '''
    Function:
        * Reads itunes data from s3
        * Processes itunes data from S3
    
    Parameters:
        * files_to_process: dataframe of files to process (updated in DTO_Script_1 execution)
        * input_bucket_name: Bucket name of raw data files.

    Returns:
        * Dataframe of processed itunes data.
    '''
    try:
        df_itunes = read_data_from_s3_itunes(files_to_process, input_bucket_name)
        itunes_monthly_data = DtoDataProcessItunes('Itunes', df_itunes)
        return itunes_monthly_data.process_data_source()
    except Exception as e:
        logging.error(f"An error occurred during iTunes data processing: {e}")
        return pd.DataFrame()



# Read and process google data (for DTO_Script_2)
def process_google_data(files_to_process, input_bucket_name):
    '''
    Function:
        * Reads google data from s3
        * Processes google data from S3
    
    Parameters:
        * files_to_process: dataframe of files to process (updated in DTO_Script_1 execution)
        * input_bucket_name: Bucket name of raw data files.

    Returns:
        * Dataframe of processed google data.
    '''
    try:
        df_google = read_data_from_s3_google(files_to_process, input_bucket_name)
        google_monthly_data = DtoDataProcessGoogle('Google', df_google)
        return google_monthly_data.process_data_source()
    except Exception as e:
        logging.error(f"An error occurred during Google data processing: {e}")
        return pd.DataFrame()



# Merge dataframes into one dataframe
def merge_data(df_amazon, df_itunes, df_google):
    '''
    Function:
        * Merges data by normalizing column names.

    Parameters:
        * df_amazon: Processed amazon dataframe.
        * df_itunes: Processed itunes dataframe.
        * df_google: Processed google dataframe.
    
    Returns:
        * Merged dataframe
    '''
    try:
        df_merged = merge_dataframes(df_amazon=df_amazon, df_itunes=df_itunes, df_google=df_google)
        logging.info(f"Dataframes merging successful")
        return df_merged
        
    except Exception as e:
        logging.error(f"An error occurred during data merging: {e}")
        raise RuntimeError("Failed to Merge Datasets.") from e



# Write processed data in s3
def write_data_to_s3(df, bucket_name, file_key):
    """
    Function:
        * Writes processed data to S3 bucket in PARTNER > YEAR > MONTH format.
        * Appends processed metadata into list
    
    Parameters:
        * df: DataFrame of processed / transformed data.
        * bucket_name: S3 bucket of output
        * file_key: File prefix of output folder.
        
    """
    enable_overwrite = False
    try:
        for partner, group in df.groupby('PARTNER'):
            for (year, month), data in group.groupby([
                pd.to_datetime(group['TRANSACTION_DATE']).dt.year,
                pd.to_datetime(group['TRANSACTION_DATE']).dt.month
            ]):
                # Define file name and key
                file_name = f"partner={partner}/year={year}/{year}-{month}.csv" 
                file_key_name = f"{file_key}/{file_name}"
                
                # Collect metadata for this file
                transaction_date = f"{year}-{month:02d}"

                _collect_processed_metadata(data,
                                            bucket_name,
                                            file_key_name,
                                            transaction_date, 
                                            partner
                                            )
                
                _collect_processed_metric_metadata(data, 
                                                   transaction_date, 
                                                   partner
                                                   )

                path = f's3://{bucket_name}/{file_key_name}'
                data.to_csv(path, index=False, mode='w')
                logging.info(f"Data writing to S3 is successful for file{file_name}.")
    except Exception as e:
        logging.error(f"An error occurred while writing data to S3: {e}")
        raise RuntimeError("Failed to write data to S3.") from e
    
#-------------------------------------------------------------------------------------------------------------------------

# Trigger DTO_Script_2 glue job
def trigger_script_2():
    try:
        glue_client = boto3.client('glue')
        glue_job_name = 'DTO_Script_2.1'
                    
        # Start the Glue job
        response = glue_client.start_job_run(JobName=glue_job_name)
        logging.info("Glue Job started:", response)
    except Exception as e:
       logging.error("Error triggering Glue job:", e)
       upload_log_file_to_s3(log_file_path, log_file_bucket_name, log_file_key)
       
