![image](https://github.com/AbhijeetPatil161097/abhijeetpatil/assets/157622300/bac4a27c-a92f-4eef-9eec-778baefda3d9)


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
ETL Pipeline have 2 Glue Jobs.
**1. Glue job Name: DTO_Script_1**
**2. Glue job Name: DTO_Script_2**

**1. Glue job Name: DTO_Script_1**
**Function :**
- DTO_Script_1 is scheduled to run daily.
- It collects metadata of all raw files available and creates a dataframe.
- Then it compares the processed metadata file from previous execution of DTO_Script_2 and current raw metadata and find all new files which are added.
- If there are new files availabble to process, it appends csv file, new_files_to_process.csv with the metadata of all new files.
- After successful appending, DTO_Script_2 is triggered.
- raw_metadata.csv file is over written to S3.

**2. Glue job Name: DTO_Script_2**
**Function :**
- DTO_Script_2 is triggered by DTO_Script_1.
- DTO_Script_2 fetches data processing scripts from S3 for each partner.
- new_files_to_process.csv is read and dataframe is created.
- This DataFrame is passed to each partner scripts.
- The dataframe is filtered and to get files of each partner.
- Data processing scripts return processed dataframe for each partner.
- These dataframes are merged into one dataframe.
- Currency conversion rates are mapped in dataframe and RETAIL_PRICE and REVENUE in USD currency is calculated.
- After successful mapping, data is written into S3 with directory PARTNER/ YEAR/ MONTH
- Processed Metadata and Metric Metadata is appended to S3.


## ✅ Test Cases for the ETL pipeine.
1. Process new files during monthly run.
2. When script changes, all data files will be processed.
3. If new data file contains old dates, those rows will be filtered out.

## 🏫 Account configuration for access
- **Account Name:** AE-AWS-RESEARCH
- **Account ID:** 932196625283


## 📧 Contact Information
- For more details check confluence documnet
  - **Link:** https://aenetworks365-my.sharepoint.com/:w:/g/personal/abhijeet_patil_aenetworks_com/EevWaRepjblHhTP3YRwb2vUBjfHSH8j8U1_s72Npf-1BGA?email=abhijeet.patil%40aenetworks.com&e=HQsTZC

- If you have any questions, feedback, or need assistance, contact us:

  - **Email:** [abhijeet.Patil@aenetworks.com](mailto:Abhijeet.Patil@aenetworks.com)


---

This README provides a comprehensive overview of the DTO Research project, detailing its components, processes, and architecture.
