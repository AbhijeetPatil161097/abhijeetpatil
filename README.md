# Project Name: DTO Research 

## Overview: 
### To automate the process of data mining, data processing and data loading to minimize the manual intervention and save time.

## Versions of Libraries:
### Following are the version of libraries used
- Python	: 3.11.5  
- pandas	: 2.0.3  
- numpy	: 1.24.3  
- os	: 3.11.5  
- re	: 2.2.1  
- s3fs	: 2023.4.0  
- boto3	: 1.34.41  
- logging	: 0.5.1.2  
- gzip	: 3.11.0  
- logging	: 0.5.1.2

## Architecture: 
### Step 1: AWS Glue Job: 
### Job Name: dto_data_processing 
1. This is the main script which orchestrates the entire ETL process. 
2. It has number of functions which run sequentially to perform required tasks:
#### Importing Libraries: 
- Required libraries are imported to perform tasks. 

#### Define Paths: 
- Paths for required assets like raw data, data processing scripts, log file, processed file, output directory are set in different variables. 

#### Initiate Glue Job: 
- Glue job is initiated/started on spark architecture. 

#### Orchestration of Glue Job Functions: 

##### 1. initialize_logging: 
- This function starts log file. 

##### 2. read_processed_files: 
- This function reads file names from processed_files.txt 

##### 3. read_script_from_s3: 
- This function reads data processing scripts for Amazon, iTunes, Google and Data Integration script. 
- Path: s3://cdr-research/Projects/DTO/dto-scripts/ 

##### 4. Execute Data Processing Scripts: 
- Scripts are executed. 

##### 5. read_data_from_s3_amazon: 
- This function reads raw amazon data stored in S3 bucket. 
- This function filters out any old files present in processed_files.txt and reads only new files.  
- Path: s3://azv-s3str-pmsa1/ dto_individual_partners/amazon/monthly/ 

##### 6. read_data_from_s3_itunes: 
- This function reads raw iTunes data stored in S3 bucket at: 
- This function filters out any old files present in processed_files.txt and reads only new files.  
- Path: s3://azv-s3str-pmsa1/ dto_individual_partners/itunes/monthly/ 

##### 7. read_data_from_s3_google: 
- This function reads raw data google stored in S3 bucket at: 
- This function filters out any old files present in processed_files.txt and reads only new files.  
- Path: s3://azv-s3str-pmsa1/ dto_individual_partners/google/monthly/ 

##### 8. Raw data Transformation: 
- This performs data transformation using data processing scripts and stores data in different variables. 
- DtoDataProcessAmazon, DtoDataProcessItunes, DtoDataProcessGoogle are classes present in individual data processing scripts. 
- Path: s3://cdr-research/Projects/DTO/dto-scripts/ 

##### 9. Merge All data: 
- ‘merge_dataframes’ function is present in data integration script.  
- Path: s3://cdr-research/Projects/DTO/dto-scripts/dto_integration_script.py 

##### 10. read_transaction_dates_from_s3 
- This function reads transaction_dates.txt file which contains all unique transaction dates for each vendor which occurred in previous glue job run. 
- Path: s3://cdr-research/Projects/DTO/transaction_dates.txt 

##### 11. Filter out old transaction dates rows: 
- This code filters out / removes transaction dates present in transaction_dates.txt file to avoid overwriting data during writing output data. 

##### 12. write_transaction_dates_to_file 
- This function puts new transaction dates to transaction_dates.txt file. 
- Path: s3://cdr-research/Projects/DTO/transaction_dates.txt 

##### 13. get_last_reporting_start_date_rows 
- Currency data is available at 
- Path: s3://cdr-research/Projects/DTO/Currency/data_0_0_0.csv.gz 
- Data is transformed to fetch only last date recorded in column ‘reporting_start_date’ for each country. 

##### 14. map_conversion_rates: 
- This function maps conversion rates from currency data to main merged data frame named: final_df

##### 15. map_revenue_usd: 
- This function maps values of REVENUE and COST in USD currency in final_df. 

##### 16. write_data_to_s3: 
- This Function writes/uploads transformed data to s3 bucket in directory  
- VENDOR_NAME > YEAR > MONTH 
- Path: s3://cdr-research/Projects/DTO/Output/ 

##### 17. write_processed_files: 
- This function appends names of files processed in current glue job run to s3 bucket. 
- Path: s3://cdr-research/Projects/DTO/processed_files.txt 

##### 18. upload_log_file_to_s3 
- This function uploads log file for current glue job run to s3 bucket. 
- Path: s3://cdr-research/Projects/DTO/glue_job_log.txt 

### Automated Glue Job Triggers: 
#### 1. Script Update: 
- Scripts are stored at GitHub repository
- Link: https://github.com/AbhijeetPatil161097/abhijeetpatil/tree/2abca9634315402e4a7bbdf0287bb3077194920a/dto-scripts 
- These Scripts are automatically fetched to S3 bucket if any changes are made to GitHub repository prod branch scripts. 
- Path: s3://cdr-research/Projects/DTO/dto-scripts/ 
- Lambda function is created to check changes at S3 bucket. 
- If files are updated at s3 path: s3://cdr-research/Projects/DTO/dto-scripts/, lambda function will clear processed_files.txt and trigger glue job which will process all raw files. 
- Lambda Function Name: dto_processing_trigger  

#### 2. Monthly Trigger: 
- Name: EventBridge (CloudWatch Events): Monthly-Trigger 
- Monthly trigger is created to run the glue job on the 5th day of each month. 
- This will process only new raw files uploaded to s3 bucket.
 

 

