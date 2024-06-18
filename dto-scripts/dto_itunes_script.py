def read_data_from_s3_itunes(files_to_process, bucket_name):
    try:
        logging.info("Processing files for Itunes")
        partner='itunes'
        
        '''
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
        df_list_itunes = []

        itunes_files_df = _filter_partner_files(files_to_process, partner)
        if itunes_files_df.empty:
            logging.error("No new Itunes files to process.")
            return pd.DataFrame()

        for index, row in itunes_files_df.iterrows():
            try:
                s3_url = row['raw_file_path']
                file_key = s3_url.split(f's3://{bucket_name}/')[1]
                logging.info(f"Processing started for file: {file_key}")

                file_name = os.path.basename(file_key)
                file_extension = os.path.splitext(file_key)[1].lower()

                file_name_month = _extract_date_from_file_key(file_key, partner)

                df = _read_file_from_s3(bucket_name, file_key, file_extension)
                df = df.dropna(subset = ['Title', 'Vendor Identifier'])
                #df = df.rename(columns=rename_mapping)

                if df is None:
                    logging.error(f"Itunes file is empty or could not be read: {file_key}")
                    continue
                logging.info(f"Step 1 Completed")
                df['Start Date'] = pd.to_datetime(df['Start Date'], format = '%m/%d/%Y')
                df['End Date'] = pd.to_datetime(df['End Date'], format = '%m/%d/%Y')
                logging.info(f"Step 2 Completed")
                # Raise error if difference between Start Date and End Date is more  than 45 Days.
                date_diff = (df['End Date'] - df['Start Date']).dt.days
                logging.info(f"Step 3 Completed")
                if (date_diff > 45).any():
                    logging.error("Begin Date and End Date difference is more than 1 month.")

                logging.info(f"Step 4 Completed")
                unique_months = ','.join(df['Start Date'].unique())
                file_info = s3.info(f"{bucket_name}/{file_key}")
                file_creation_date = file_info['LastModified'].strftime('%Y-%m-%d')
                logging.info(f"Step 5 Completed")
                file_row_count = len(df)
                metrics= ['QUANTITY (Quantity)', 'REVENUE_NATIVE (Extended Partner Share)']
                raw_file_values = [df['Quantity'].astype('int').sum(), 
                                   df['Extended Partner Share'].astype('float').sum()
                                  ]

                _collect_file_metadata(bucket_name, 
                                       file_key, 
                                       file_name, 
                                       file_creation_date,
                                       file_name_month,
                                       partner,
                                       unique_months,
                                       file_row_count
                                      )
                logging.info(f"Step 6 Completed")
                _collect_metric_metadata(bucket_name,
                                         file_key, 
                                         file_name, 
                                         partner,
                                         unique_months, metrics= metrics, 
                                         raw_file_values = raw_file_values
                                        )

                df_list_itunes.append(df)
                logging.info(f"Processing completed for file: {file_key}")
                logging.info(f"Step 7 Completed")
            except Exception as e:
                logging.error(f"An error occurred while reading Itunes data: {e}")

        df_itunes = pd.concat(df_list_itunes, ignore_index=True)
        raw_metadata = pd.DataFrame(new_raw_metadata)
        df_itunes_filtered = _remove_associated_files(df_itunes, itunes_files_df, raw_metadata, partner)
        
        '''
        column_name = ['Asset/Content Flavor', 'Primary Genre', 'Provider Country', 'Sales or Return']
        for column in column_name:
            if column not in df_itunes_filtered.columns:
                df_itunes_filtered[column] = None
        '''

        logging.info("Amazon DataFrame created successfully")
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
            #'ASSET/CONTENT FLAVOR': lambda x: '|'.join(sorted(pd.Series.unique(x))),
            'PARTNER_TITLE': lambda x: '%%'.join(pd.Series.unique(x))
        }

        
        
    def drop_columns(self, columns_to_drop):
        try:
            existing_columns = [col for col in columns_to_drop if col in self.df.columns]
            self.df.drop(columns=existing_columns, inplace=True)
        except Exception as e:
            raise RuntimeError(f"Error dropping columns: {e}")
    
    def new_title(self, row, title_columns):
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
        try:
            titles = [str(row[col]).replace('"', '').strip() for col in title_columns]
            return ' || '.join(titles)
        except KeyError as e:
            raise RuntimeError(f"Error processing title: {e}")

    def process_new_title_and_drop_columns(self, title_columns, new_title_col, new_partner_col):
        try:
            self.df[new_title_col] = self.df.apply(lambda row: self.new_title(row, title_columns), axis=1)
            self.df[new_partner_col] = self.df.apply(lambda row: self.new_partner_title(row, title_columns), axis=1)
            self.df[new_title_col] = self.df[new_title_col].fillna('').apply(lambda x: x.replace('|', '').strip())
            self.df.drop(columns=title_columns, inplace=True)
        except KeyError as e:
            raise RuntimeError(f"Error dropping old title columns: {e}")


            
    def replace_titles(self, territory_col, sku_col, new_title_col):
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
    
    def calculate_metric_values(self, unit_retail_price, retail_price, quantity_col, revenue_col, unit_revenue_col):
        try:
            self.df[revenue_col] = self.df[unit_revenue_col] * self.df[quantity_col].abs()
            self.df[retail_price] = self.df[unit_retail_price] * self.df[quantity_col].abs()
            
        except Exception as e:
            raise RuntimeError(f"Error calculating revenue: {e}")
            
    def aggregate_data(self, groupby_columns, agg_columns):
        try:
            self.df = self.df.groupby(groupby_columns).agg(agg_columns).reset_index()
        except KeyError as e:
            raise RuntimeError(f"Error aggregating data: {e}")
            
    def calculate_weigted_mean(self, unit_retail_price, retail_price, quantity_col, revenue_col, unit_revenue_col):
        try:
            self.df[unit_retail_price] = self.df[retail_price] / self.df[quantity_col]
            self.df[unit_revenue_col] = self.df[revenue_col] / self.df[quantity_col]
        except Exception as e:
            raise RuntimeError(f"Error calculating weighted mean: {e}")
    
    def rename_columns(self):
        try:
            self.df.rename(columns=lambda x: x.upper().replace(' ', '_'), inplace=True)
        except Exception as e:
            raise RuntimeError(f"Error renaming columns: {e}")
    
    def add_columns_to_df(self, vendor_name):
        try:
            self.df.insert(0, 'PARTNER', vendor_name)
            self.df['REVENUE_USD'] = np.nan
            self.df['IS_CONVERSION_RATE'] = False
            return self.df
        except Exception as e:
            raise RuntimeError(f"Error adding vendor name column: {e}")

    def process_data_source(self):
        try:
            self.drop_columns(self.columns_to_drop)
            self.process_new_title_and_drop_columns(self.title_columns, self.new_title_col, self.new_partner_col)
            self.replace_titles(self.territory_col, self.sku_col, self.new_title_col)
            
            self.calculate_metric_values(self.unit_retail_price, 
                                         self.retail_price, 
                                         self.quantity_col, 
                                         self.revenue_col, 
                                         self.unit_revenue_col
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
