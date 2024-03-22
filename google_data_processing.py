#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import re

class DtoDataProcessGoogle:
    # Define column names as class variables
    FILE_NAME = 'FILE_NAME'
    HEADER = 'HEADER'
    FOOTER = 'FOOTER'
    COUNTRY = 'COUNTRY'
    TRANSACTION_DATE = 'TRANSACTION_DATE'
    NAME_OF_TITLE = 'NAME_OF_TITLE'
    SEASON_TITLE = 'SEASON_TITLE'
    SHOW_TITLE = 'SHOW_TITLE'
    CHANNEL_NAME = 'CHANNEL_NAME'
    VIDEO_CATEGORY = 'VIDEO_CATEGORY'
    YOUTUBE_VIDEO_ID = 'YOUTUBE_VIDEO_ID'
    PLAY_ASSET_ID = 'PLAY_ASSET_ID'
    EIDR_TITLE_ID = 'EIDR_TITLE_ID'
    EIDR_EDIT_ID = 'EIDR_EDIT_ID'
    PARTNER_REPORTING_ID = 'PARTNER_REPORTING_ID'
    TRANSACTION_IMPORT_SOURCE = 'TRANSACTION_IMPORT_SOURCE'
    TRANSACTION_TYPE = 'TRANSACTION_TYPE'
    RESOLUTION = 'RESOLUTION'
    REFUND_CHARGEBACK = 'REFUND_CHARGEBACK'
    COUPON_USED = 'COUPON_USED'
    CAMPAIGN_ID = 'CAMPAIGN_ID'
    PURCHASE_LOCATION = 'PURCHASE_LOCATION'
    RETAIL_PRICE_USD = 'RETAIL_PRICE_USD'
    NATIVE_RETAIL_PRICE_AUD = 'NATIVE_RETAIL_PRICE_AUD'
    TAX_EXCLUSIVE_RETAIL_PRICE_USD = 'TAX_EXCLUSIVE_RETAIL_PRICE_USD'
    NATIVE_TAX_EXCLUSIVE_RETAIL_PRICE_AUD = 'NATIVE_TAX_EXCLUSIVE_RETAIL_PRICE_AUD'
    TOTAL_TAX_USD = 'TOTAL_TAX_USD'
    NATIVE_TOTAL_TAX = 'NATIVE_TOTAL_TAX'
    PARTNER_EARNINGS_FRACTION = 'PARTNER_EARNINGS_FRACTION'
    CONTRACTUAL_MINIMUM_PARTNER_EARNINGS_USD = 'CONTRACTUAL_MINIMUM_PARTNER_EARNINGS_USD'
    NATIVE_CONTRACTUAL_MINIMUM_PARTNER_EARNINGS_AUD = 'NATIVE_CONTRACTUAL_MINIMUM_PARTNER_EARNINGS_AUD'
    PARTNER_EARNINGS_USING_TAX_EXCLUSIVE_RETAIL_PRICE_USD = 'PARTNER_EARNINGS_USING_TAX_EXCLUSIVE_RETAIL_PRICE_USD'
    NATIVE_PARTNER_EARNINGS_USING_TAX_EXCLUSIVE_RETAIL_PRICE_AUD = 'NATIVE_PARTNER_EARNINGS_USING_TAX_EXCLUSIVE_RETAIL_PRICE_AUD'
    PARTNER_FUNDED_DISCOUNTS_USD = 'PARTNER_FUNDED_DISCOUNTS_USD'
    NATIVE_PARTNER_FUNDED_DISCOUNTS_AUD = 'NATIVE_PARTNER_FUNDED_DISCOUNTS_AUD'
    FINAL_PARTNER_EARNINGS_USD = 'FINAL_PARTNER_EARNINGS_USD'
    NATIVE_FINAL_PARTNER_EARNINGS_AUD = 'NATIVE_FINAL_PARTNER_EARNINGS_AUD'
    CONVERSION_RATE = 'CONVERSION_RATE'
    CONVERSION_DATE = 'CONVERSION_DATE'
    MONTH_YEAR = 'MONTH_YEAR'
    QUANTITY = 'QUANTITY'


    def __init__(self, platform, df):
        self.platform = platform
        self.df = df
        
        self.columns_to_drop = [self.FILE_NAME, self.HEADER, self.FOOTER, self.EIDR_TITLE_ID, self.EIDR_EDIT_ID,
                                self.TRANSACTION_IMPORT_SOURCE, self.REFUND_CHARGEBACK, 
                                self.COUPON_USED, self.CAMPAIGN_ID, self.NATIVE_RETAIL_PRICE_AUD,
                                self.NATIVE_CONTRACTUAL_MINIMUM_PARTNER_EARNINGS_AUD, self.NATIVE_PARTNER_EARNINGS_USING_TAX_EXCLUSIVE_RETAIL_PRICE_AUD,
                                self.NATIVE_PARTNER_FUNDED_DISCOUNTS_AUD, self.NATIVE_FINAL_PARTNER_EARNINGS_AUD, self.MONTH_YEAR,
                                self.PLAY_ASSET_ID, self.CONVERSION_RATE, self.FINAL_PARTNER_EARNINGS_USD, self.PARTNER_FUNDED_DISCOUNTS_USD,
                               self.PARTNER_EARNINGS_USING_TAX_EXCLUSIVE_RETAIL_PRICE_USD, self.PARTNER_EARNINGS_FRACTION]
        
        self.title_columns = [self.SHOW_TITLE, self.SEASON_TITLE, self.NAME_OF_TITLE]
        
        self.groupby_columns = [self.COUNTRY, 'NEW_TITLE', self.YOUTUBE_VIDEO_ID, self.TRANSACTION_DATE, self.CONVERSION_DATE, self.PARTNER_REPORTING_ID]
        
        self.metric_columns = [self.RETAIL_PRICE_USD, self.TAX_EXCLUSIVE_RETAIL_PRICE_USD, self.TOTAL_TAX_USD]
        for item in self.metric_columns:
            self.df[item] = pd.to_numeric(self.df[item], errors='coerce')

        self.df.fillna(value={col: 0 for col in self.metric_columns}, inplace=True)
        self.date_columns = {'transaction_date': self.TRANSACTION_DATE, 'month_year' : self.MONTH_YEAR, 'conversion_date' : self.CONVERSION_DATE}
    
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
    def new_title(self, row, title_columns=None):
        try:
            title_columns = title_columns or self.title_columns
            series_title = str(row[self.SHOW_TITLE]).replace('"', '').strip()
            season_title = str(row[self.SEASON_TITLE]).replace('"', '').strip()
            title = str(row[self.NAME_OF_TITLE]).replace('"', '').strip()

            if series_title in title:
                series_title_part = series_title.replace(season_title, '').strip()
                if season_title in title:
                    season_title_part = season_title.replace(series_title, '').strip()
                    new_title = f"{series_title} | {season_title_part} | {title}".strip()
                else:
                    new_title = f"{series_title} | {title}".strip()
            else:
                new_title = f"{series_title} | {title}".strip()

            new_title = re.sub(' +', ' ', new_title)
            new_title = new_title.replace('| |', '|')
            new_title = new_title.strip('|')
            return new_title
        except Exception as e:
            print(f"Error processing title: {e}")

                   
    def process_new_title_and_drop_columns(self, title_columns=None):
        try:
            title_columns = title_columns or self.title_columns
            # Add 'NEW_TITLE' column
            self.df['NEW_TITLE'] = self.df.apply(lambda row: self.new_title(row, title_columns), axis=1)

            # Drop unnecessary columns
            for column in title_columns:
                if column in self.df.columns:
                    self.df = self.df.drop(column, axis=1)
        except Exception as e:
            print(f"Error dropping old title columns: {e}")
            

    # Aggregate metric data for similar rows
    def aggregate_data(self):
        try:
            self.df['QUANTITY'] = 1

            def unique_join(series):
                return '|'.join(pd.Series.unique(series))
            agg_columns = {
                self.RETAIL_PRICE_USD : 'mean', 
                self.TAX_EXCLUSIVE_RETAIL_PRICE_USD : 'mean',
                self.TOTAL_TAX_USD : 'mean',
                self.QUANTITY : 'sum', 
                'RESOLUTION' : unique_join,
                'TRANSACTION_TYPE' : unique_join,
                'PURCHASE_LOCATION' : unique_join,
                'VIDEO_CATEGORY' : unique_join,
                'CHANNEL_NAME' : unique_join      
            }

            self.df = self.df.groupby(self.groupby_columns).agg(agg_columns).reset_index()
        except KeyError as e:
            print(f"Error aggregating data: {e}")
        except Exception as e:
            print(f"Unexpected error aggregating data: {e}")
    def formatting_transaction_date(self):
        try:
            self.date_columns['transaction_date'] = pd.to_datetime(self.df[self.date_columns['transaction_date']]).dt.date
        except Exception as e:
            print(f"Error formatting transaction date: {e}")

    def formatting_conversion_date(self):
        try:
            self.date_columns['conversion_date'] = pd.to_datetime(self.df[self.date_columns['conversion_date']]).dt.date
        except Exception as e:
            print(f"Error formatting conversion date: {e}")

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
            self.formatting_conversion_date()
            return self.add_vendor_name_column()
        except Exception as e:
            print(f"Error processing data source: {e}")

