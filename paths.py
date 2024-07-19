'''
Defining necessary paths
'''

# Log file paths
log_file_bucket_name = 'cdr-research'
log_file_key = 'Projects/DTO/Misc/glue_job_log.txt'

# Matadata files paths
metadata_bucket = 'cdr-research'

raw_metadata_file_key = 'Projects/DTO/Metadata/raw_metadata.csv'
processed_metadata_file_key = 'Projects/DTO/Metadata/processed_metadata.csv'
metric_file_key = 'Projects/DTO/Metadata/metric_validation.csv'

# Files to process path
files_to_process_file_key = 'Projects/DTO/Misc/new_files_to_process.csv'

# Input data paths
input_bucket_name = 'azv-s3str-pmsa1'

input_folder_key_amazon = 'dto_individual_partners/amazon/monthly/'
input_folder_key_itunes = 'dto_individual_partners/itunes/monthly/revenue/'
input_folder_key_google = 'dto_individual_partners/google/monthly/'

# Scripts path
script_bucket_name = 'cdr-research'
script_keys = [
    'Projects/DTO/dto-scripts/dto_amazon_script.py',
    'Projects/DTO/dto-scripts/dto_itunes_script.py',
    'Projects/DTO/dto-scripts/dto_google_script.py',
    'Projects/DTO/dto-scripts/dto_integration_script.py'
]

# Output data paths
output_bucket_name = 'cdr-research'
output_folder_key = 'Projects/DTO/Output/test/'

# Currency data paths
currency_bucket_name = 'cdr-research'
currency_file_key = 'Projects/DTO/Currency/data_0_0_0.csv.gz'