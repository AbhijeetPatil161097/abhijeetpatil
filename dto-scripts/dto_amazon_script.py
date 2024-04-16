#!/usr/bin/env python
# coding: utf-8

# In[ ]:

#qqq
import numpy as np
import pandas as pd

class DtoDataProcessAmazon:
    
    # Define column names as class variables
    DISC_PLUS = 'Disc Plus'
    TRANSACTION = 'Transaction'
    CATEGORY = 'Category'
    TRANSACTION_FORMAT = 'Transaction Format'
    MEDIA_FORMAT = 'Media Format'
    SKU_NUMBER = 'SKU Number'
    TITLE = 'Title'
    SEASON_TITLE = 'Season Title'
    SERIES_TITLE = 'Series Title'
    DVD_STREET_DATE = 'DVD Street Date'
    THEATRICAL_RELEASE_DATE = 'Theatrical Release Date'
    RETAIL_PRICE = 'Retail Price'
    UNIT_COST = 'Unit Cost'
    EPISODE_NUMBER = 'Episode Number'
    CYS_DISCOUNT = 'CYS Discount'
    CYS_EPISODE_COUNT = 'CYS Episode Count'
    QUANTITY = 'Quantity'
    REVENUE = 'Revenue'
    COST = 'Cost'
    VENDOR_CODE = 'Vendor Code'
    TERRITORY = 'Territory'
    CONVERSION_DATE = 'Conversion Date'
    TRANSACTION_DATE = 'TRANSACTION_DATE'
    
    

    def __init__(self, platform, df):
        self.platform = platform
        self.df = df
        try:
            self.df['TRANSACTION_DATE'] = pd.to_datetime(self.df['TRANSACTION_DATE'], format='%Y-%m').dt.strftime('%m-%Y')
        except Exception as e:
            raise ValueError(f"Error converting TRANSACTION_DATE column: {e}")
        
        # Define columns to drop
        self.columns_to_drop = [
            self.DVD_STREET_DATE, self.THEATRICAL_RELEASE_DATE, self.EPISODE_NUMBER, self.VENDOR_CODE,
            self.DISC_PLUS, self.CYS_EPISODE_COUNT
        ]
        
        # Define groupby columns
        self.groupby_columns = [
            self.SKU_NUMBER, self.TERRITORY, self.TRANSACTION_DATE, 'NEW_TITLE', self.TRANSACTION_FORMAT, 
        ]
        
        # Define metric columns
        self.metric_columns =[self.RETAIL_PRICE, self.UNIT_COST, self.QUANTITY, self.REVENUE, self.COST]
        for item in self.metric_columns:
            try:
                self.df[item] = pd.to_numeric(self.df[item])
            except Exception as e:
                raise ValueError(f"Error converting {item} column to numeric: {e}")

            
    # Drop Redundant Columns
    def drop_columns(self, columns_to_drop=None):
        try:
            self.df.drop(columns=[col for col in self.columns_to_drop if col in self.df.columns], inplace=True)
            return self.df
        except Exception as e:
            raise ValueError(f"Error dropping columns: {e}")
            
            
    # Concat Title, Season Title and Series Title to NEW_TITLE        
    def new_title(self, row):
        try:
            series_title = str(row[self.SERIES_TITLE]).replace('"', '').strip() if not pd.isna(row[self.SERIES_TITLE]) else ''
            season_title = str(row[self.SEASON_TITLE]).replace('"', '').strip() if not pd.isna(row[self.SEASON_TITLE]) else ''
            title = str(row[self.TITLE]).replace('"', '').strip() if not pd.isna(row[self.TITLE]) else ''

            if series_title in season_title:   
                if season_title in title:
                    return f"{title}"
                else:
                    return f"{season_title} | {title}"

            elif season_title in title:
                    return f"{series_title} | {title}"

            else:
                return f"{series_title} | {season_title} | {title}"
            
        except KeyError as e:
            raise KeyError(f"Error processing title: {e}")    
            

    # Add NEW_TITLE column and delete old title columns       
    def process_new_title_and_drop_columns(self):
        try:
            self.df['NEW_TITLE'] = self.df.apply(lambda row: self.new_title(row), axis=1)
            self.df.drop(columns=[self.SERIES_TITLE, self.SEASON_TITLE, self.TITLE], inplace=True)
            self.df['NEW_TITLE'] = self.df['NEW_TITLE'].fillna('').apply(lambda x: x.replace('|', '').strip())

            return self.df
        except KeyError as e:
            raise KeyError(f"Error dropping old title columns: {e}")
            
    
    # Replace non english titles with most frequent English Titles
    def replace_titles(self, territory_filter='US', sku_number_filter=' ', title_column='NEW_TITLE'):
        try:
            mask = (self.df[self.TERRITORY] == territory_filter) & (self.df[self.SKU_NUMBER] != sku_number_filter)
            filtered_data = self.df[mask]

            grouped_titles = filtered_data.groupby(self.SKU_NUMBER)[title_column]
            most_frequent_titles = grouped_titles.apply(lambda x: x.mode().iat[0] if not x.empty else x.iat[0]).reset_index(name=title_column)

            merged_data = self.df.merge(most_frequent_titles, on=self.SKU_NUMBER, how='left', suffixes=('_x', '_y'))
            merged_data[title_column] = merged_data.pop(title_column + '_y').combine_first(merged_data.pop(title_column + '_x')).str.strip()

            self.df = merged_data
            return self.df

        except KeyError as e:
            raise KeyError(f"Error replacing titles - KeyError: {e}")
            

    # Remove Quotations from SKUs
    def remove_quotations(self):
        try:
            self.df[self.SKU_NUMBER] = self.df[self.SKU_NUMBER].str.replace('"', '')
            return self.df
        except KeyError as e:
            raise KeyError(f"Error removing quotations: {e}")
            
            
    # Calculate Revenue        
    def calculate_revenue(self):
        try:
            self.df[self.REVENUE] = self.df[self.RETAIL_PRICE] * self.df[self.QUANTITY].abs()
            return self.df
        except KeyError as e:
            raise KeyError(f"Error calculating revenue: {e}")
            
            
    # Aggregate rows
    def aggregate_data(self):
        try:
            def unique_join(series):
                return '|'.join(pd.Series.unique(series))
            agg_columns = {
                self.RETAIL_PRICE: 'mean', 
                self.UNIT_COST: 'mean',
                self.QUANTITY: 'sum', 
                self.REVENUE: 'sum', 
                self.COST: 'sum',
                self.MEDIA_FORMAT : unique_join
            }

            self.df = self.df.groupby(self.groupby_columns).agg(agg_columns).reset_index()
            return self.df
        except KeyError as e:
            raise KeyError(f"Error aggregating data: {e}")
            
            
    # Rename Columns        
    def rename_columns(self):
        try:
            self.df = self.df.rename(columns=lambda x: x.upper().replace(' ', '_'))
            return self.df
        except Exception as e:  
            raise ValueError(f"Error renaming columns: {e}")
            
    # Add AMAZON as vendor name
    def add_vendor_name_column(self):
        try:
            self.df.insert(0, 'VENDOR_NAME', 'AMAZON')
            return self.df
        except KeyError as e:
            raise KeyError(f"Error adding vendor name column: {e}")

            
    # Create a function to call all above functions
    def process_data_source(self):
        try:
            self.drop_columns()
            self.process_new_title_and_drop_columns()
            self.replace_titles()
            self.remove_quotations()
            self.calculate_revenue()
            self.aggregate_data()
            self.rename_columns()
            return self.add_vendor_name_column()
        
        except Exception as e:
            raise ValueError(f"Error processing data source: {e}")

