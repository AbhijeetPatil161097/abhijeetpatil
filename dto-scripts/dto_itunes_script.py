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
    try:
        logging.info("Processing files for Itunes")
        partner='itunes'
        
        '''
        We have sales and revenue data in itunes raw data.
        Sales and Revenue have two different schemas. For  current script, we are not using sales data.
        Below rename mapping to be used if Sales data is required to process.
        
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
        '''
        
        # Empty list to store Itunes DataFrames of each file.
        df_list_itunes = []
        
        # Filter itunes files from files_to_process dataframe. ('_filter_partner_files' Function defined in DTO_Script_2.py) 
        itunes_files_df = _filter_partner_files(files_to_process, partner)
        if itunes_files_df.empty:
            logging.error("No new Itunes files to process.")
            return pd.DataFrame()
        
        # Iterating each row to get s3 url of file.
        for index, row in itunes_files_df.iterrows():
            try:
                s3_url = row['raw_file_path']
                file_key = s3_url.split(f's3://{bucket_name}/')[1]
                logging.info(f"Processing started for file: {file_key}")

                file_name = os.path.basename(file_key)
                file_extension = os.path.splitext(file_key)[1].lower()
                
                # Extract date from file name. ('_extract_date_from_file_key' - Function defined in DTO_Script_2.py)
                file_name_month = _extract_date_from_file_key(file_key, partner)
                
                # Read data  from raw file and create DataFrame. ('_read_file_from_s3' - Function defined in DTO_Script_2.py)
                df = _read_file_from_s3(bucket_name, file_key, file_extension)
                df = df.dropna(subset = ['Title', 'Vendor Identifier'])
                #df = df.rename(columns=rename_mapping)

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
                
                # Get metric data for data processing and aggregation validation.
                metrics= ['QUANTITY (Quantity)', 'REVENUE_NATIVE (Extended Partner Share)']
                raw_file_values = [df['Quantity'].astype('int').sum(), 
                                   df['Extended Partner Share'].astype('float').sum()
                                  ]

                # Collect metric data of raw files. ('_collect_file_metadata' - Function defined in DTO_Script_2.py)
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
                
                # Append DataFrame in list. ('df_list_itunes' - List defined in DTO_Script_2.py)
                df_list_itunes.append(df)
                logging.info(f"Processing completed for file: {file_key}")
            except Exception as e:
                logging.error(f"An error occurred while reading Itunes data: {e}")
        
        # Concat DataFrames
        df_itunes = pd.concat(df_list_itunes, ignore_index=True)
        raw_metadata = pd.DataFrame(new_raw_metadata)
        
        # Remove all files associated with partner and month of non processed files to prevent incomplete data processing.
        # '_remove_associated_files' - Function defined in DTO_Script_2.py
        df_itunes_filtered = _remove_associated_files(df_itunes, itunes_files_df, raw_metadata, partner)
        
        '''
        column_name = ['Asset/Content Flavor', 'Primary Genre', 'Provider Country', 'Sales or Return']
        for column in column_name:
            if column not in df_itunes_filtered.columns:
                df_itunes_filtered[column] = None
        '''

        logging.info("Itunes DataFrame created successfully")
        return df_itunes_filtered
        
    except Exception as e:
        logging.error(f"An error occurred while reading Itunes data: {e}")
        return pd.DataFrame()


class DtoDataProcessItunes:
    def __init__(self, platform, df):
        self.platform = platform
        self.df = df.copy()
        
        '''                            
        # As the raw data is generated by partner and output should be from the perspective of A+E, we are renaming
          the mteric columns.
        Royalty Price is what A+E gets from partner so it becomes Unit Revenue Native for A+E.
        
        '''
        self.column_rename_map = {
            'Customer Price' : 'Unit Retail Price Native',
            'Partner Share' : 'Unit Revenue Native',
            'Extended Partner Share' : 'Revenue Native'
        }
        
        self.df.rename(columns=self.column_rename_map, inplace=True)
        self.sales_return_mapping = {'S': 'SALES', 'R': 'RETURN'}
        
        self.columns_to_drop = [
            'Provider', 'UPC', 'ISRC', 'Label/Studio/Network', 
            'Product Type Identifier', 'Customer Currency', 'PreOrder', 
            'ISAN', 'Apple Identifier', 'CMA', 'Vendor Offer Code', 'Grid', 'Promo Code',
            'Parent Identifier', 'Parent Type Id', 'Extended Partner Share', 'Region', 'End Date',
            'Provider Country'
        ]
        self.title_columns = ['Artist/Show/Developer/Author', 'Title']
        self.new_title_col = 'NEW_TITLE'
        self.new_partner_col = 'PARTNER_TITLE'
        self.sales_or_return = 'Sales or Return'
        self.unit_retail_price = 'Unit Retail Price Native'
        self.unit_revenue_col = 'Unit Revenue Native'
        self.revenue_col = 'Revenue Native'
        self.quantity_col = 'Quantity'
        self.retail_price = 'Retail Price Native'
        self.territory_col = 'Country Of Sale'
        self.sku_col = 'Vendor Identifier'
        self.groupby_columns = ['Vendor Identifier', 'Country Of Sale', 'NEW_TITLE', 'Start Date']
        self.agg_columns = {
            'Unit Retail Price Native': 'mean', 
            'Unit Revenue Native': 'mean', 
            'Quantity': 'sum', 
            'Revenue Native': 'sum', 
            'Retail Price Native': 'sum', 
            #'ASSET/CONTENT FLAVOR': lambda x: '|'.join(sorted(pd.Series.unique(x))), # only available in sales data
            'Sales or Return': lambda x: '|'.join(pd.Series.unique(x)),
            'PARTNER_TITLE': lambda x: '%%'.join(pd.Series.unique(x))
        }

        self.na_str_value = '_NA_'
        
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
            series_title = str(row[title_columns[0]]).replace('"', '').strip()
            title = str(row[title_columns[1]]).replace('"', '').strip()

            if series_title in title:
                series_title_part = series_title[len(series_title):].strip()
                return f"{series_title_part} | {title}"
            else:
                return f"{series_title} | {title}"
        except KeyError as e:
            raise RuntimeError(f"Error processing title: {e}")
    
    
    def new_partner_title(self, row, title_columns):
        '''Create partner title by merging all title columns and using (||) as a seperator.'''
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
            self.df[new_title_col] = self.df[new_title_col].fillna('').apply(lambda x: x.replace('|', '').strip())
            self.df.drop(columns=title_columns, inplace=True)
        except KeyError as e:
            raise RuntimeError(f"Error dropping old title columns: {e}")


            
    def replace_titles(self, territory_col, sku_col, new_title_col):
        '''By matching Vendor Identifier, replace all non english titles by US titles.'''
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
    
    
    def map_sales_return(self, sales_or_return):
        '''
        Function:
            * Renames catagorical column values of column SALES_OR_RETURN.
            * Map value _NA_ for null cells.
        
        '''
        try:
            self.df[sales_or_return] = self.df[sales_or_return].map(self.sales_return_mapping)
            self.df[sales_or_return].fillna(self.na_str_value, inplace=True)
        except Exception as e:
            raise RuntimeError(f"Error mapping sales/return: {e}")

    
    
    def calculate_metric_values(self, unit_retail_price, retail_price, quantity_col):
        '''
            Calculate retail price by multiplying unit_retail_price to quantity
            
            Using absolute of quantity to account for returns or refunds.
            
        '''
        try:
            self.df.loc[self.df[self.unit_retail_price] < 0, self.unit_retail_price] *= -1
            
            self.df[retail_price] = self.df[unit_retail_price] * self.df[quantity_col]
            
        except Exception as e:
            raise RuntimeError(f"Error calculating revenue: {e}")
            
            
    def aggregate_data(self, groupby_columns, agg_columns):
        '''Aggregate data on on groupby_columns'''
        try:
            self.df = self.df.groupby(groupby_columns).agg(agg_columns).reset_index()
        except KeyError as e:
            raise RuntimeError(f"Error aggregating data: {e}")
            
            
    def calculate_weigted_mean(self, unit_retail_price, retail_price, quantity_col, revenue_col, unit_revenue_col):
        '''Calculating weighted mean of unit_retail_price and unit_revenue'''
        try:
            self.df[unit_retail_price] = self.df[retail_price] / self.df[quantity_col]
            self.df[unit_revenue_col] = self.df[revenue_col] / self.df[quantity_col]
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
            raise RuntimeError(f"Error adding vendor name column: {e}")

            
    def process_data_source(self):
        '''Calling all function in specific order.'''
        try:
            self.drop_columns(self.columns_to_drop)
            self.process_new_title_and_drop_columns(self.title_columns, self.new_title_col, self.new_partner_col)
            self.replace_titles(self.territory_col, self.sku_col, self.new_title_col)
            self.map_sales_return(self.sales_or_return)
            self.calculate_metric_values(self.unit_retail_price, 
                                         self.retail_price, 
                                         self.quantity_col, 
                                        )
            self.calculate_weigted_mean(self.unit_retail_price, 
                                         self.retail_price, 
                                         self.quantity_col, 
                                         self.revenue_col, 
                                         self.unit_revenue_col
                                        )
            self.aggregate_data(self.groupby_columns, self.agg_columns)
            self.rename_columns()
            return self.add_columns_to_df('itunes')
        except RuntimeError as e:
            raise e
