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
    try:
        logging.info(f"Processing files for Google")
        partner = 'google'
        
        # Empty list to store Google DataFrames of each file.
        df_list_google = []
        
        # For each country, column names have currency in brackets. eg for (USD), (AUD) 
        # To concat all country data, removing brackets from column names.
        pattern = re.compile(r'\s*\(.+')
        
        # Filter google files from files_to_process dataframe. ('_filter_partner_files' Function defined in DTO_Script_2.py)
        google_files_df = _filter_partner_files(files_to_process, partner)
        if google_files_df.empty:
            logging.error("No new Google files to process.")
            return pd.DataFrame()
        
        # Iterating each row to get s3 url of file.
        for index, row in google_files_df.iterrows():
            s3_url = row['raw_file_path']
            file_key = s3_url.split(f's3://{input_bucket_name}/')[1]
            logging.info(f"Processing started for file: {file_key}")

            file_name = os.path.basename(file_key)
            file_extension = os.path.splitext(file_key)[1]
            
            # Extract date from file name. ('_extract_date_from_file_key' - Function defined in DTO_Script_2.py)
            file_name_month = _extract_date_from_file_key(file_key, partner)
            
            # Read data  from raw file and create DataFrame. ('_read_file_from_s3' - Function defined in DTO_Script_2.py)
            df = _read_file_from_s3(bucket_name, file_key, file_extension)                          
            if df is None:
                logging.error(f"Google file is empty: {file_key}")
                continue
            
            # As there are multiple redundant rows above actual header row, removing such rows.
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
            
            # Get metric data for data processing and aggregation validation.
            metrics= ['QUANTITY (QUANTITY)',
                      'UNIT_REVENUE_NATIVE (Native Partner Earnings Using Tax Exclusive Retail Price)'
                     ]
            raw_file_values = [df['QUANTITY'].astype('int').sum(), 
                               df['Native Partner Earnings Using Tax Exclusive Retail Price'].astype('float').sum()
                              ]
            
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
            
            # Append DataFrame in list. ('df_list_google' - List defined in DTO_Script_2.py)
            df_list_google.append(df)
            logging.info(f"Processing completed for file: {file_key}")
        
        # Concat DataFrame
        df_google = pd.concat(df_list_google, ignore_index=True)
        raw_metadata = pd.DataFrame(new_raw_metadata)
        
        # Remove all files associated with partner and month of non processed files to prevent incomplete data processing.
        # '_remove_associated_files' - Function defined in DTO_Script_2.py
        df_google_filtered = _remove_associated_files(df_google, google_files_df, raw_metadata, partner)
        
        return df_google_filtered
                                     
    except Exception as e:
        logging.error(f"An error occurred while reading Google data: {e}")
        return pd.DataFrame()
    
    
class DtoDataProcessGoogle:
    def __init__(self, platform, df):
        self.platform = platform
        self.df = df.copy()
        
        '''                            
        # As the raw data is generated by partner and output should be from the perspective of A+E, we are renaming
          the mteric columns.
        Native Partner Earnings Using Tax Exclusive Retail Pricee is what A+E gets from partner so 
        it becomes Unit Revenue Native for A+E.
        
        '''
        self.column_rename_map = {
            'Retail Price' : 'Unit Retail Price USD',
            'Native Retail Price' : 'Unit Retail Price Native',
            'Native Partner Earnings Using Tax Exclusive Retail Price' : 'Unit Revenue Native',
            'Partner Earnings Using Tax Exclusive Retail Price' : 'Unit Revenue USD'
        }
        
        self.df.rename(columns=self.column_rename_map, inplace=True)
        
        self.columns_to_drop = [
            'EIDR Title ID', 'Transaction Import Source', 'Tax Exclusive Retail Price', 
            'Coupon Used', 'Campaign ID', 'Total Tax', 'Native Tax Exclusive Retail Price',
            'Native Contractual Minimum Partner Earnings', 'Native Total Tax',
            'Native Partner Earnings Using Tax Exclusive Retail Price', 'Partner Earnings Fraction', 
            'Native Partner Funded Discounts', 'Native Final Partner Earnings', 'Contractual Minimum Partner Earnings',
            'Contractual Minimum Partner Earnings', 'Partner Funded Discounts',
            'Native Retail Currency', 'Native Partner Currency'
        ]
        self.title_columns = ['Channel Name', 'Show Title', 'Season Title', 'Name of Title']
        self.territory_col = 'Country'
        self.sku_col = 'YouTube Video ID'
        self.new_title_col = 'NEW_TITLE'
        self.new_partner_col = 'PARTNER_TITLE'
        
        self.unit_retail_price_native = 'Unit Retail Price Native'
        self.unit_revenue_native = 'Unit Revenue Native'
        self.unit_retail_price_usd = 'Unit Retail Price USD'
        self.unit_revenue_usd = 'Unit Revenue USD'
        self.quantity_col = 'QUANTITY'
        self.revenue_usd = 'Revenue USD'
        self.revenue_native = 'Revenue Native'
        self.retail_price_native = 'Retail Price Native'
        self.retail_price_usd = 'Retail Price USD'
        
        self.conversion_rate_col = 'Conversion Rate'
        self.groupby_columns = ['Country', 'NEW_TITLE', 'YouTube Video ID', 'Transaction Date']
        self.agg_columns = {
            'Unit Retail Price Native': 'mean',
            'Unit Retail Price USD' : 'mean',
            'Unit Revenue Native': 'mean',
            'Unit Revenue USD' : 'mean',
            'Retail Price Native' : 'sum',
            'Retail Price USD' : 'sum',
            'Revenue Native' : 'sum',
            'Revenue USD' : 'sum',
            'QUANTITY': 'sum', 
            'Conversion Rate': 'mean',
            'Resolution': lambda x: '|'.join(sorted(map(str, pd.Series.unique(x)))),
            'Transaction Type': lambda x: '|'.join(sorted(map(str, pd.Series.unique(x)))),
            'Purchase Location': lambda x: '|'.join(sorted(map(str, pd.Series.unique(x)))),
            'Video Category': lambda x: '|'.join(sorted(map(str, pd.Series.unique(x)))),
            'PARTNER_TITLE': lambda x: '%%'.join(pd.Series.unique(x))
        }
        
        self.numeric_columns = [self.unit_retail_price_native, self.unit_revenue_native,
            self.unit_retail_price_usd, self.unit_revenue_usd,
            self.quantity_col, self.conversion_rate_col
        ]
        
    def convert_columns_to_float(self, columns):
        '''Convert numeric columns to float'''
        for column in columns:
            if column in self.df.columns:
                self.df[column] = self.df[column].astype(float)
        
        
    def drop_columns(self, columns_to_drop):
        '''Drop redundant columns from DataFrame'''
        try:
            existing_columns = [col for col in columns_to_drop if col in self.df.columns]
            self.df.drop(columns=existing_columns, inplace=True)
        except Exception as e:
            raise RuntimeError(f"Error dropping columns: {e}")

    
    def new_title(self, row, title_columns):
        '''Create new title by merging and normalizing all title columns'''
        try:
            def clean_title(title):
                return re.sub(r'"|:| {2,}|,|-', '', str(title)).strip()
            
            series_title = clean_title(row['Series Title'])
            season_title = clean_title(row['Season Title'])
            title = clean_title(row['Title'])

            
            
            channel_name = clean_title(row['Channel Name'])
            series_title = clean_title(row['Show Title'])
            season_title = clean_title(row['Series Title'])
            title = clean_title(row['Name of Title'])
            
            if channel_name in series_title:
                if series_title in season_title:   
                    if season_title in title:
                        return title
                    else:
                        return f"{season_title} | {title}"
                elif season_title in title:
                    return f"{series_title} | {title}"
                
                else:
                    return f"{series_title} | {season_title} | {title}"
            else:
                return f"{channel_name} | {series_title} | {season_title} | {title}"
            
        except KeyError as e:
            raise RuntimeError(f"Error processing title: {e}")
    
    def new_partner_title(self, row, title_columns):
        '''Create partner title using (||) as a seperator.'''
        try:
            titles = [str(row[col]).replace('"', '').strip() for col in title_columns]
            return ' || '.join(titles)
        except KeyError as e:
            raise RuntimeError(f"Error processing title: {e}")

    def process_new_title_and_drop_columns(self, title_columns, new_title_col, new_partner_col):
        '''Add new title and partner title in DataFrame and remove old title columns.'''
        try:
            self.df[new_title_col] = self.df.apply(lambda row: self.new_title(row, title_columns), axis=1)
            self.df[new_partner_col] = self.df.apply(lambda row: self.new_partner_title(row, title_columns), axis=1)
            self.df[new_title_col] = self.df[new_title_col].fillna('').apply(lambda x: x.replace('  ', '').strip())
            self.df.drop(columns=title_columns, inplace=True)
        except KeyError as e:
            raise RuntimeError(f"Error dropping old title columns: {e}")

    def replace_titles(self, territory_col, sku_col, new_title_col):
        '''By matching YouTube Video ID, replace all non english titles by US titles.'''
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
            raise KeyError(f"Error replacing titles - KeyError: {e}")
    
    
    def calculate_metric_values(self, quantity_col):
        '''
            Calculate Retail Price and Revenue by multiplying with quantity.
            
        '''
        try:
            self.df[self.retail_price_usd] = self.df[self.unit_retail_price_usd] * self.df[self.quantity_col]
            self.df[self.retail_price_native] = self.df[self.unit_retail_price_native] * self.df[self.quantity_col]
            self.df[self.revenue_native] = self.df[self.unit_revenue_native] * self.df[self.quantity_col].abs()
            self.df[self.revenue_usd] = self.df[self.unit_revenue_usd] * self.df[self.quantity_col].abs()
            
        except KeyError as e:
            raise KeyError(f"Error calculating metric values: {e}")
    
            
    def aggregate_data(self, groupby_columns, agg_columns):
        '''Aggregate data on groupby_columns'''
        try:
            self.df = self.df.groupby(groupby_columns).agg(agg_columns).reset_index()
        except KeyError as e:
            raise RuntimeError(f"Error aggregating data: {e}")
            
        
    def calculate_weigted_mean(self, quantity_col):
        '''Calculating weighted mean of unit_retail_price and unit_revenue'''
        try:
            mask = self.df[self.quantity_col] != 0
            self.df.loc[mask, self.unit_retail_price_native] = self.df.loc[mask, self.retail_price_native] / self.df.loc[mask, quantity_col]
            self.df.loc[mask, self.unit_retail_price_usd] = self.df.loc[mask, self.retail_price_usd]  / self.df.loc[mask, quantity_col]
            self.df.loc[mask, self.unit_revenue_native] = self.df.loc[mask, self.revenue_native] / self.df.loc[mask, quantity_col]
            self.df.loc[mask, self.unit_revenue_usd] = self.df.loc[mask, self.revenue_usd] / self.df.loc[mask, quantity_col]
        except Exception as e:
            raise RuntimeError(f"Error calculating revenue: {e}")
    
    
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
            * Adding IS_CONVERSION_RATE = False, as conversion rate is not present in raw data.
        '''
        try:
            self.df.insert(0, 'PARTNER', vendor_name)
            self.df['IS_CONVERSION_RATE'] = True
            return self.df
        except Exception as e:
            raise RuntimeError(f"Error adding vendor name column: {e}")

            
    def process_data_source(self):
        '''Calling all function in specific order.'''
        try:
            self.convert_columns_to_float(self.numeric_columns)
            self.drop_columns(self.columns_to_drop)
            self.process_new_title_and_drop_columns(self.title_columns, self.new_title_col, self.new_partner_col)
            self.replace_titles(self.territory_col, self.sku_col, self.new_title_col)
            self.calculate_metric_values(self.quantity_col)
            self.aggregate_data(self.groupby_columns, self.agg_columns)
            self.calculate_weigted_mean(self.quantity_col)
            self.rename_columns()
            return self.add_columns_to_df('google')
        except RuntimeError as e:
            raise e  
