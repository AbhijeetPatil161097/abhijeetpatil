import os
import logging
import pandas as pd
import numpy as np
import s3fs

fs = s3fs.S3FileSystem()

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
    
    try:
        logging.info("Processing files for Amazon")
        
        partner = 'amazon'
        
        # Empty list to store DataFrames of each file.
        df_list_amazon = []
        
        # Filter amazon files from files_to_process dataframe. ('_filter_partner_files' Function defined in DTO_Script_2.py)
        amazon_files_df = _filter_partner_files(files_to_process, partner)
        
        if amazon_files_df.empty:
            logging.error("No new Amazon files to process.")
            return pd.DataFrame()
        
        # Iterating each row to get s3 url of file.
        for index, row in amazon_files_df.iterrows():
            s3_url = row['raw_file_path']
            file_key = s3_url.split(f's3://{bucket_name}/')[1]
            logging.info(f"Processing started for file: {file_key}")

            file_name = os.path.basename(file_key)
            file_extension = os.path.splitext(file_key)[1].lower()
            
            # Read data  from raw file and create DataFrame. ('_read_file_from_s3' - Function defined in DTO_Script_2.py)
            df = _read_file_from_s3(bucket_name, file_key, file_extension)
            if df is None:
                logging.error(f"Amazon file is empty or could not be read: {file_key}")
                continue
                
            # Extract date from file name. ('_extract_date_from_file_key' - Function defined in DTO_Script_2.py)
            file_name_month = _extract_date_from_file_key(file_key, partner)
            df['TRANSACTION_DATE'] = file_name_month
            
            # Get unique months from DataFrame.
            unique_months = ','.join(df['TRANSACTION_DATE'].unique())
            file_info = s3.info(f"{bucket_name}/{file_key}")
            file_creation_date = file_info['LastModified'].strftime('%Y-%m-%d')
            file_row_count = len(df)
            
            # Get metric data for data processing and aggregation validation.
            metrics= ['QUANTITY (Quantity)', 'REVENUE_NATIVE (Cost)']
            raw_file_values = [df['Quantity'].astype('float').sum(), 
                               df['Cost'].astype('float').sum()]
            
            # Collect metadata of raw files. ('_collect_file_metadata' - Function defined in DTO_Script_2.py)
            _collect_file_metadata(bucket_name, 
                                   file_key, 
                                   file_name, 
                                   file_creation_date,
                                   file_name_month,
                                   partner,
                                   unique_months,
                                   file_row_count
                                  )
            
            # Collect metric data of raw files. ('_collect_metric_metadata' - Function defined in DTO_Script_2.py)
            _collect_metric_metadata(bucket_name,
                                     file_key, 
                                     file_name, 
                                     partner,
                                     unique_months, metrics= metrics, 
                                     raw_file_values = raw_file_values
                                    )
            
            # Append DataFrame in list. ('df_list_amazon' - List defined in DTO_Script_2.py)
            df_list_amazon.append(df)
            
            logging.info(f"Processing completed for file: {file_key}")
        
        # Concat DataFrame
        df_amazon = pd.concat(df_list_amazon, ignore_index=True)
        raw_metadata = pd.DataFrame(new_raw_metadata)
        
        # Remove all files associated with partner and month of non processed files to prevent incomplete data processing.
        # '_remove_associated_files' - Function defined in DTO_Script_2.py
        df_amazon_filtered = _remove_associated_files(df_amazon, amazon_files_df, raw_metadata, partner)

        logging.info("Amazon DataFrame created successfully")
        return df_amazon_filtered

    except Exception as e:
        logging.error(f"An error occurred while reading Amazon data: {e}")
        return pd.DataFrame()
    
class DtoDataProcessAmazon:
    def __init__(self, platform, df):
        self.platform = platform
        self.df = df.copy()
        
        '''                            
        # As the raw data is generated by partner and output should be from the perspective of A+E, we are renaming
          the mteric columns.
        Unit Cost is what A+E gets from partner so it becomes Unit Revenue for A+E.
        
        '''
        self.column_rename_map = {
            'Retail Price': 'Unit Retail Price Native',
            'Revenue': 'Retail Price Native',
            'Unit Cost': 'Unit Revenue Native',
            'Cost': 'Revenue Native'
        }
        
        self.df.rename(columns=self.column_rename_map, inplace=True)          
        self.columns_to_drop = [
            'Disc Plus', 'Transaction', 'CYS Discount', 'Category', 
            'DVD Street Date', 'Theatrical Release Date', 'Episode Number', 'Vendor Code'
        ]
        self.title_columns = ['Series Title', 'Season Title', 'Title']
        self.new_title_col = 'NEW_TITLE'
        self.new_partner_col = 'PARTNER_TITLE'
        self.territory_col = 'Territory'
        self.sku_col = 'SKU Number'
        
        self.unit_retail_price = 'Unit Retail Price Native'
        self.retail_price = 'Retail Price Native'
        self.unit_revenue = 'Unit Revenue Native'
        self.revenue_col = 'Revenue Native'
        self.quantity_col = 'Quantity'
        
        self.groupby_columns = ['SKU Number', 'Territory', 'TRANSACTION_DATE', 'NEW_TITLE', 'Transaction Format', 'CYS Episode Count']
        self.agg_columns = {
            'Unit Retail Price Native': 'mean', 
            'Unit Revenue Native': 'mean', 
            'Quantity': 'sum', 
            'Revenue Native': 'sum', 
            'Retail Price Native': 'sum', 
            'Media Format': lambda x: '|'.join(sorted(pd.Series.unique(x))),
            'PARTNER_TITLE': lambda x: '%%'.join(sorted(pd.Series.unique(x))),
            'Transaction': lambda x: '%%'.join(sorted(pd.Series.unique(x)))
        }
        

    def drop_columns(self, columns_to_drop):
        '''Drop redundant columns from DataFrame'''
        try:
            existing_columns = [col for col in columns_to_drop if col in self.df.columns]
            self.df.drop(columns=existing_columns, inplace=True)
        except Exception as e:
            raise RuntimeError(f"Error dropping columns: {e}")
    
    def remove_quotations(self, sku_col):
        '''Remove quotation marks (") from SKU Number column'''
        try:
            self.df[sku_col] = self.df[sku_col].str.replace('"', '')
            self.df[sku_col] = self.df[sku_col].replace(' ', np.nan)
            return self.df
        except KeyError as e:
            raise KeyError(f"Error removing quotations: {e}")
    
    def new_title(self, row, title_columns):
        '''Create new title by merging and normalizing all title columns'''
        try:
            series_title = str(row['Series Title']).replace('"', '').strip()
            season_title = str(row['Season Title']).replace('"', '').strip()
            title = str(row['Title']).replace('"', '').strip()

            if series_title in season_title:   
                if season_title in title:
                    return title
                else:
                    return f"{season_title} | {title}"
            elif season_title in title:
                return f"{series_title} | {title}"
            else:
                return f"{series_title} | {season_title} | {title}"
        except KeyError as e:
            raise RuntimeError(f"Error processing  new_title: {e}")
    
    def new_partner_title(self, row, title_columns):
        '''Create partner title by merging all title columns and using (||) as a seperator.'''
        try:
            titles = [str(row[col]).replace('"', '').strip() for col in title_columns]
            return ' || '.join(titles)
        except KeyError as e:
            raise RuntimeError(f"Error processing pertner_title: {e}")

    def process_new_title_and_drop_columns(self, title_columns, new_title_col, new_partner_col):
        '''Add new title and partner title in DataFrame and remove old title columns.'''
        try:
            self.df[new_title_col] = self.df.apply(lambda row: self.new_title(row, title_columns), axis=1)
            self.df[new_partner_col] = self.df.apply(lambda row: self.new_partner_title(row, title_columns), axis=1)
            self.df[new_title_col] = self.df[new_title_col].fillna('').apply(lambda x: x.replace('|', '').strip())
            self.df.drop(columns=title_columns, inplace=True)
        except KeyError as e:
            raise RuntimeError(f"Error adding new title or dropping old title columns in DataFrame: {e}")

    def replace_titles(self, territory_col, sku_col, new_title_col):
        '''By matching SKU Number, replace all non english titles by US titles.'''
        try:
            mask = (self.df[territory_col] == 'US') & (self.df[sku_col] != np.nan)
            filtered_data = self.df[mask]

            grouped_titles = filtered_data.groupby(sku_col)[new_title_col]
            most_frequent_titles = grouped_titles.apply(lambda x: x.mode()[0] if not x.empty else x.iloc[0]).reset_index(name=new_title_col)

            merged_data = self.df.merge(most_frequent_titles, on=sku_col, how='left', suffixes=('_x', '_y'))
            merged_data[new_title_col] = merged_data.pop(new_title_col + '_y').combine_first(merged_data.pop(new_title_col + '_x')).str.strip()

            self.df = merged_data
            return self.df
        except KeyError as e:
            raise KeyError(f"Error replacing titles: {e}")
            
    def calculate_retail_price(self, unit_retail_price, retail_price, quantity_col):
        '''
            Calculate retail price by multiplying unit_retail_price to quantity
            Using absolute of quantity to account for returns or refunds.
            
        '''
        try:
            self.df[retail_price] = self.df[unit_retail_price] * self.df[quantity_col].abs()
        except Exception as e:
            raise RuntimeError(f"Error calculating retail price: {e}")
            
    def aggregate_data(self, groupby_columns, agg_columns):
        '''Aggregate data on on groupby_columns'''
        try:
            null_sku = self.df[self.df[self.sku_col].isna()]
            non_null_sku = self.df[~self.df[self.sku_col].isna()]
            
            non_null_sku[self.unit_retail_price] = non_null_sku[self.unit_retail_price].abs()
            non_null_sku[self.unit_revenue] = non_null_sku[self.unit_revenue].abs()
            
            aggregated_df = non_null_sku.groupby(groupby_columns).agg(agg_columns).reset_index()
            self.df = pd.concat([aggregated_df, null_sku])
            return self.df
        except KeyError as e:
            raise RuntimeError(f"Error aggregating data: {e}")
            
            
    def calculate_weighted_mean(self, unit_retail_price, unit_revenue, quantity_col, revenue_col, retail_price):
        '''Calculating weighted mean of unit_retail_price and unit_revenue'''
        try:
            mask = self.df[quantity_col] != 0
            self.df.loc[mask, unit_retail_price] = self.df.loc[mask, retail_price] / self.df.loc[mask, quantity_col]
            self.df.loc[mask, unit_revenue] = self.df.loc[mask, revenue_col] / self.df.loc[mask, quantity_col]
            
        except Exception as e:
            raise RuntimeError(f"Error calculating weighted mean: {e}")
            
    
    def rename_columns(self):
        '''Rename column name by Capitalizing and replacing empty space by underscore.'''
        try:
            self.df.rename(columns=lambda x: x.upper().replace(' ', '_'), inplace=True)
        except Exception as e:
            raise RuntimeError(f"Error renaming columns: {e}")
    
    
    def add_columns_to_df(self, vendor_name):
        '''
            Add necessary columns in DataFrame.
            * Adding PARTNER column to distinguish data after merging with other partners.
            * Adding REVENUE_USD to calculate it later.
            * Adding IS_CONVERSION_RATE = False, as conversion rate is not present in raw data.
        '''
        try:
            self.df.insert(0, 'PARTNER', vendor_name)
            self.df['REVENUE_USD'] = np.nan
            self.df['IS_CONVERSION_RATE'] = False
            return self.df
        except Exception as e:
            raise RuntimeError(f"Error adding columns in Amazon DataFrame: {e}")
            
            

    def process_data_source(self):
        '''Calling all function in specific order.'''
        try:
            self.drop_columns(self.columns_to_drop)
            self.remove_quotations(self.sku_col)
            self.process_new_title_and_drop_columns(self.title_columns, 
                                                    self.new_title_col, 
                                                    self.new_partner_col
                                                   )
            self.replace_titles(self.territory_col, self.sku_col, self.new_title_col)
            self.calculate_retail_price(self.unit_retail_price,
                                        self.retail_price,
                                        self.quantity_col
                                       )
            self.aggregate_data(self.groupby_columns, self.agg_columns)
            self.calculate_weighted_mean(self.unit_retail_price, 
                                         self.unit_revenue, 
                                         self.quantity_col, 
                                         self.revenue_col, 
                                         self.retail_price
                                        )
            self.rename_columns()
            return self.add_columns_to_df('amazon')
        
        except RuntimeError as e:
            raise RuntimeError(f"Error in processing Amazon Data: {e}")
