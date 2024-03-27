#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import re

class DtoDataProcessGoogle:
    # Define column names as class variables
    COUNTRY = 'Country'
    TRANSACTION_DATE = 'Transaction Date'
    NAME_OF_TITLE = 'Name of Title'
    SEASON_TITLE = 'Season Title'
    SHOW_TITLE = 'Show Title'
    CHANNEL_NAME = 'Channel Name'
    VIDEO_CATEGORY = 'Video Category'
    YOUTUBE_VIDEO_ID = 'YouTube Video ID'
    PLAY_ASSET_ID = 'Play Asset ID'
    EIDR_TITLE_ID = 'EIDR Title ID'
    EIDR_EDIT_ID = 'EIDR Edit ID'
    PARTNER_REPORTING_ID = 'Partner Reporting ID'
    TRANSACTION_IMPORT_SOURCE = 'Transaction Import Source'
    TRANSACTION_TYPE = 'Transaction Type'
    RESOLUTION = 'Resolution'
    REFUND_CHARGEBACK = 'Refund/Chargeback'
    COUPON_USED = 'Coupon Used'
    CAMPAIGN_ID = 'Campaign ID'
    PURCHASE_LOCATION = 'Purchase Location'
    RETAIL_PRICE_USD = 'Retail Price'
    NATIVE_RETAIL_PRICE_AUD = 'Native Retail Price'
    TAX_EXCLUSIVE_RETAIL_PRICE_USD = 'Tax Exclusive Retail Price'
    NATIVE_TAX_EXCLUSIVE_RETAIL_PRICE_AUD = 'Native Tax Exclusive Retail Price'
    TOTAL_TAX_USD = 'Total Tax'
    NATIVE_TOTAL_TAX = 'Native Total Tax'
    PARTNER_EARNINGS_FRACTION = 'Partner Earnings Fraction'
    CONTRACTUAL_MINIMUM_PARTNER_EARNINGS_USD = 'Contractual Minimum Partner Earnings'
    NATIVE_CONTRACTUAL_MINIMUM_PARTNER_EARNINGS_AUD = 'Native Contractual Minimum Partner Earnings'
    PARTNER_EARNINGS_USING_TAX_EXCLUSIVE_RETAIL_PRICE_USD = 'Partner Earnings Using Tax Exclusive Retail Price'
    NATIVE_PARTNER_EARNINGS_USING_TAX_EXCLUSIVE_RETAIL_PRICE_AUD = 'Native Partner Earnings Using Tax Exclusive Retail Price'
    PARTNER_FUNDED_DISCOUNTS_USD = 'Partner Funded Discounts'
    NATIVE_PARTNER_FUNDED_DISCOUNTS_AUD = 'Native Partner Funded Discounts'
    FINAL_PARTNER_EARNINGS_USD = 'Final Partner Earnings'
    NATIVE_FINAL_PARTNER_EARNINGS_AUD = 'Native Final Partner Earnings'
    CONVERSION_RATE = 'Conversion Rate'
    NATIVE_RETAIL_CURRENCY = 'Native Retail Currency'
    NATIVE_PARTNER_CURRENCY = 'Native Partner Currency'
    QUANTITY = 'QUANTITY'
    
    def __init__(self, platform, df):
        self.platform = platform
        self.df = df
        
        # Columns to drop
        self.columns_to_drop = [
            self.EIDR_TITLE_ID, self.EIDR_EDIT_ID, self.TRANSACTION_IMPORT_SOURCE,
            self.REFUND_CHARGEBACK, self.COUPON_USED, self.CAMPAIGN_ID,
            self.NATIVE_RETAIL_PRICE_AUD, self.NATIVE_CONTRACTUAL_MINIMUM_PARTNER_EARNINGS_AUD,
            self.NATIVE_PARTNER_EARNINGS_USING_TAX_EXCLUSIVE_RETAIL_PRICE_AUD,
            self.NATIVE_PARTNER_FUNDED_DISCOUNTS_AUD, self.NATIVE_FINAL_PARTNER_EARNINGS_AUD
        ]
        
        # Title columns
        self.title_columns = [self.SHOW_TITLE, self.SEASON_TITLE, self.NAME_OF_TITLE]
        
        # Groupby columns
        self.groupby_columns = [
            self.COUNTRY, 'NEW_TITLE', self.YOUTUBE_VIDEO_ID, 
            self.TRANSACTION_DATE, self.PARTNER_REPORTING_ID
        ]
        
        # Metric columns
        self.metric_columns = [
            self.RETAIL_PRICE_USD, self.TAX_EXCLUSIVE_RETAIL_PRICE_USD, self.TOTAL_TAX_USD
        ]
        
        # Pattern for column names
        self.pattern = re.compile(r'\s*\(.+')
        
        # Date columns
        self.date_columns = {self.TRANSACTION_DATE: 'Transaction Date', 'MONTH_YEAR': 'MONTH_YEAR'}

        # Process metric columns
        for item in self.metric_columns:
            self.df[item] = pd.to_numeric(self.df[item], errors='coerce')

        self.df.fillna(value={col: 0 for col in self.metric_columns}, inplace=True)
        
        # Set Quantity Column
        self.df['QUANTITY'] = 1
        
        
        # Drop Redundant columns
    def drop_columns(self, columns_to_drop=None):
        try:
            columns_to_drop = columns_to_drop or self.columns_to_drop
            existing_columns = [col for col in columns_to_drop if col in self.df.columns]
            self.df.drop(columns=existing_columns, inplace=True)
        except Exception as e:
            print(f"Error dropping columns: {e}")
            
            
    # Remove Special Characters       
    def remove_special_characters_from_columns(self, title_columns=None):
        try:
            title_columns = title_columns or self.title_columns
            for column in title_columns:
                if column in self.df.columns:
                    self.df[column] = self.df[column].apply(lambda x: re.sub('[^a-zA-Z0-9\s]', '', str(x)))
        except Exception as e:
            print(f"Error removing special characters: {e}")
    
    
    
    #  Concat Titles
    def new_title(self, row):
        try:
            series_title = str(row[self.SHOW_TITLE]).replace('"', '').strip()
            season_title = str(row[self.SEASON_TITLE]).replace('"', '').strip()
            title = str(row[self.NAME_OF_TITLE]).replace('"', '').strip()

            if series_title and season_title:
                if series_title in season_title:
                    season_part = season_title[len(series_title):].strip()
                    return f"{series_title} | {season_part} | {title}"

            return f"{series_title} {title}"
        except KeyError as e:
            print(f"Error processing title: {e}")
        except Exception as e:
            print(f"Unexpected error processing title: {e}")


                   
    def process_new_title_and_drop_columns(self):
        try:
            # Add 'NEW_TITLE' column
            self.df['NEW_TITLE'] = self.df.apply(lambda row: self.new_title(row), axis=1)

            # Drop unnecessary columns
            self.df.drop(columns=[self.SHOW_TITLE, self.SEASON_TITLE, self.NAME_OF_TITLE], inplace=True)
        except KeyError as e:
            print(f"Error dropping old title columns: {e}")
        except Exception as e:
            print(f"Unexpected error processing new title and dropping columns: {e}")
            

    # Aggregate metric data for similar rows
    def aggregate_data(self):
        try:
            def unique_join(series):
                return '|'.join(pd.Series.unique(series))
            agg_columns = {
                self.RETAIL_PRICE_USD : 'mean', 
                self.TAX_EXCLUSIVE_RETAIL_PRICE_USD : 'mean',
                self.TOTAL_TAX_USD : 'mean',
                self.QUANTITY : 'sum', 
                self.RESOLUTION : unique_join,
                self.TRANSACTION_TYPE : unique_join,
                self.PURCHASE_LOCATION : unique_join,
                self.VIDEO_CATEGORY : unique_join,
                self.CHANNEL_NAME : unique_join      
            }

            self.df = self.df.groupby(self.groupby_columns).agg(agg_columns).reset_index()
        except KeyError as e:
            print(f"Error aggregating data: {e}")
        except Exception as e:
            print(f"Unexpected error aggregating data: {e}")
            
            
    def formatting_transaction_date(self):
        try:
            self.date_columns[self.TRANSACTION_DATE] = pd.to_datetime(self.df[self.date_columns[self.TRANSACTION_DATE]]).dt.date
        except Exception as e:
            print(f"Error formatting transaction date: {e}")


    def add_vendor_name_column(self, vendor_name='GOOGLE'):
        try:
            self.df.insert(0, 'VENDOR_NAME', vendor_name)
            return self.df
        except Exception as e:
            print(f"Error adding vendor name column: {e}")
            return self.df

    def process_data_source(self):
        try: 
            self.drop_columns()
            self.remove_special_characters_from_columns()
            self.process_new_title_and_drop_columns()
            self.aggregate_data()
            self.formatting_transaction_date()
            return self.add_vendor_name_column()
        except Exception as e:
            print(f"Error processing data source: {e}")

