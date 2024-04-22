# 🌟 Project Name: DTO Research 🌟

## 🚀 Overview
To automate the process of data mining, data processing, and data loading to minimize manual intervention and save time.

### 📁 Data Processing Scripts Repository
- **Repository Link:** [DTO Scripts](https://github.com/AbhijeetPatil161097/abhijeetpatil/tree/2abca9634315402e4a7bbdf0287bb3077194920a/dto-scripts)
  - Scripts for data processing tasks are stored here.

### 🛠️ Code Pipeline
- **Pipeline Name:** dto-code-pipeline
- **Path:** s3://cdr-research/Projects/DTO/dto-scripts/
- The code pipeline automatically updates changes from the GitHub repository to the S3 bucket.

### 💱 Currency Data
- Currency data is automatically updated weekly from the Snowflake table to the S3 bucket using the Snowflake task 'export_to_s3_monthly'.
  - **Snowflake Table:** CONTENT_DB.CDR.DTO_REFERENCECURRENCY
  - **S3 File Path:** s3://cdr-research/Projects/DTO/Currency/data_0_0_0.csv.gz

### 🔔 Lambda Functions
- A lambda function named 'dto_processing_trigger' triggers the Glue job automatically when changes are made in scripts and monthly on a specific date.

### 📚 Data Catalog
- The data catalog keeps track of metadata of raw data and transformed data in the form of tables.
  - **For Raw Data:**
    - **Crawler Name:** dto_raw_data
    - **Database Name:** dto_research
    - **Table Name Prefix:** dto_raw_
  - **For Transformed Data:**
    - **Crawler Name:** dto_transformed_data
    - **Database Name:** dto_research
    - **Table Name Prefix:** dto_transformed_

## 📦 Versions of Libraries
### 📚 Library Versions
- Python: 3.11.5
- pandas: 2.0.3
- numpy: 1.24.3
- os: 3.11.5
- re: 2.2.1
- s3fs: 2023.4.0
- boto3: 1.34.41
- logging: 0.5.1.2
- gzip: 3.11.0
- logging: 0.5.1.2

## 🏗️ Architecture
## Architecture

### Step 1: AWS Glue Job - dto_data_processing

This Glue job orchestrates the entire ETL process:

1. **Initialize Logging:**
   - This function starts the log file.

2. **Read Processed Files:**
   - This function reads file names from processed_files.txt.

3. **Read Script from S3:**
   - This function reads data processing scripts for Amazon, iTunes, Google, and the Data Integration script.
   - **Path:** s3://cdr-research/Projects/DTO/dto-scripts/

4. **Execute Data Processing Scripts:**
   - Scripts are executed.

5. **Read Data from S3 - Amazon:**
   - This function reads raw Amazon data stored in the S3 bucket.
   - It filters out any old files present in processed_files.txt and reads only new files.
   - **Path:** s3://azv-s3str-pmsa1/dto_individual_partners/amazon/monthly/

6. **Read Data from S3 - iTunes:**
   - This function reads raw iTunes data stored in the S3 bucket.
   - It filters out any old files present in processed_files.txt and reads only new files.
   - **Path:** s3://azv-s3str-pmsa1/dto_individual_partners/itunes/monthly/

7. **Read Data from S3 - Google:**
   - This function reads raw Google data stored in the S3 bucket.
   - It filters out any old files present in processed_files.txt and reads only new files.
   - **Path:** s3://azv-s3str-pmsa1/dto_individual_partners/google/monthly/

8. **Raw Data Transformation:**
   - This function performs data transformation using data processing scripts and stores data in different variables.
   - DtoDataProcessAmazon, DtoDataProcessItunes, DtoDataProcessGoogle are classes present in individual data processing scripts.
   - **Path:** s3://cdr-research/Projects/DTO/dto-scripts/

9. **Merge All Data:**
   - The 'merge_dataframes' function is present in the data integration script.
   - **Path:** s3://cdr-research/Projects/DTO/dto-scripts/dto_integration_script.py

10. **Read Transaction Dates from S3:**
    - This function reads the transaction_dates.txt file, which contains all unique transaction dates for each vendor that occurred in the previous Glue job run.
    - **Path:** s3://cdr-research/Projects/DTO/transaction_dates.txt

11. **Filter Out Old Transaction Dates Rows:**
    - This code filters out/removes transaction dates present in the transaction_dates.txt file to avoid overwriting data during writing output data.

12. **Write Transaction Dates to File:**
    - This function puts new transaction dates to the transaction_dates.txt file.
    - **Path:** s3://cdr-research/Projects/DTO/transaction_dates.txt

13. **Get Last Reporting Start Date Rows:**
    - Currency data is available at
    - **Path:** s3://cdr-research/Projects/DTO/Currency/data_0_0_0.csv.gz
    - Data is transformed to fetch only the last date recorded in the column 'reporting_start_date' for each country.

14. **Map Conversion Rates:**
    - This function maps conversion rates from currency data to the main merged data frame named: final_df

15. **Map Revenue USD:**
    - This function maps values of REVENUE and COST in USD currency in final_df.

16. **Write Data to S3:**
    - This function writes/uploads transformed data to the S3 bucket in the directory VENDOR_NAME > YEAR > MONTH
    - **Path:** s3://cdr-research/Projects/DTO/Output/

17. **Write Processed Files:**
    - This function appends names of files processed in the current Glue job run to the S3 bucket.
    - **Path:** s3://cdr-research/Projects/DTO/processed_files.txt

18. **Upload Log File to S3:**
    - This function uploads the log file for the current Glue job run to the S3 bucket.
    - **Path:** s3://cdr-research/Projects/DTO/glue_job_log.txt
   





This architecture ensures efficient data processing and automation of the ETL pipeline for DTO research.

---

This README provides a comprehensive overview of the DTO Research project, detailing its components, processes, and architecture.
