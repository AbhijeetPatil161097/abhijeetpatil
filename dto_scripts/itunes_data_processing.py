#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import re

class DtoDataProcessItunes:

    # Column name variables
    FILE_NAME = 'FILE_NAME'
    HEADER = 'HEADER'
    FOOTER = 'FOOTER'
    PROVIDER = 'PROVIDER'
    PROVIDER_COUNTRY = 'PROVIDER_COUNTRY'
    VENDOR_IDENTIFIER = 'VENDOR_IDENTIFIER'
    UPC = 'UPC'
    ISRC = 'ISRC'
    ARTIST_SHOW = 'ARTIST_SHOW'
    TITLE = 'TITLE'
    LABEL_STUDIO_NETWORK = 'LABEL_STUDIO_NETWORK'
    PRODUCT_TYPE_IDENTIFIER = 'PRODUCT_TYPE_IDENTIFIER'
    UNITS = 'UNITS'
    ROYALTY_PRICE = 'ROYALTY_PRICE'
    BEGIN_DATE = 'BEGIN_DATE'
    END_DATE = 'END_DATE'
    CUSTOMER_CURRENCY = 'CUSTOMER_CURRENCY'
    COUNTRY_CODE = 'COUNTRY_CODE'
    ROYALTY_CURRENCY = 'ROYALTY_CURRENCY'
    PREORDER = 'PREORDER'
    ISAN = 'ISAN'
    APPLE_IDENTIFIER = 'APPLE_IDENTIFIER'
    CUSTOMER_PRICE = 'CUSTOMER_PRICE'
    CMA = 'CMA'
    ASSET_CONTENT_FLAVOR = 'ASSET_CONTENT_FLAVOR'
    VENDOR_OFFER_CODE = 'VENDOR_OFFER_CODE'
    GRID = 'GRID'
    PROMO_CODE = 'PROMO_CODE'
    PARENT_IDENTIFIER = 'PARENT_IDENTIFIER'
    PARENT_TYPE_ID = 'PARENT_TYPE_ID'
    PRIMARY_GENRE = 'PRIMARY_GENRE'
    CONVERSION_DATE = 'CONVERSION_DATE'

    def __init__(self, platform, df):
        self.platform = platform
        self.df = df
        self.columns_to_drop = [
            self.FILE_NAME, self.HEADER, self.FOOTER, self.PROVIDER, self.PROVIDER_COUNTRY,
            self.UPC, self.ISRC, self.APPLE_IDENTIFIER, self.PROVIDER_COUNTRY, self.ISRC,
            self.PREORDER, self.ISAN, self.CMA, self.GRID, self.PROMO_CODE, self.PARENT_IDENTIFIER,
            self.PARENT_TYPE_ID
        ]
        self.title_columns = [self.ARTIST_SHOW, self.TITLE]
        self.metric_columns = {self.CUSTOMER_PRICE: 'CUSTOMER_PRICE', self.ROYALTY_PRICE: 'ROYALTY_PRICE', self.UNITS: 'UNITS'}
        for key, value in self.metric_columns.items():
            self.df[value] = pd.to_numeric(self.df[value], errors='coerce')
        self.date_columns = {self.BEGIN_DATE: 'BEGIN_DATE', self.END_DATE: 'END_DATE', self.CONVERSION_DATE: 'CONVERSION_DATE'}
        self.currency_to_country_mapping = {'USD': 'US', 'CAD': 'CA', 'EUR': 'DE', 'GBP': 'GB', 'AUD': 'AU'}

    # Drop redundant columns
    def drop_columns(self):
        try:
            existing_columns = [col for col in self.columns_to_drop if col in self.df.columns]
            self.df.drop(columns=existing_columns, inplace=True)
        except Exception as e:
            print(f"Error dropping columns: {e}")

    # Remove special characters from Title columns
    def remove_special_characters_from_columns(self):
        try:
            for column in self.title_columns:
                if column in self.df.columns:
                    self.df[column] = self.df[column].apply(lambda x: re.sub('[^a-zA-Z0-9\s]', '', str(x)))
        except Exception as e:
            print(f"Error removing special characters: {e}")

    # Combine Title attributes and create a single title column
    def new_title(self, row):
        try:
            series_title = str(row[self.ARTIST_SHOW]).replace('"', '').strip() if self.ARTIST_SHOW in row else ''
            season_title = str(row[self.TITLE]).replace('"', '').strip() if self.TITLE in row else ''

            if series_title and season_title:
                return f"{series_title} | {season_title}"
            elif series_title:
                return series_title
            elif season_title:
                return season_title
            else:
                return ""
        except Exception as e:
            print(f"Error processing title: {e}")

    # Drop old title columns
    def process_new_title_and_drop_columns(self):
        # Add 'NEW_TITLE' column
        self.df['NEW_TITLE'] = self.df.apply(self.new_title, axis=1)

        # Drop unnecessary columns
        try:
            for column in self.title_columns:
                if column in self.df.columns:
                    self.df = self.df.drop(column, axis=1)
        except Exception as e:
            print(f"Error processing title and dropping columns: {e}")

    # Format CONVERSION_DATE column
    def convert_date_columns(self, date_format='%m/%d/%Y', new_date_format='%Y-%m-%d'):
        try:
            self.df.rename(columns=self.date_columns, inplace=True)
            self.df = self.df.apply(
                lambda col: pd.to_datetime(col, format=date_format).dt.strftime(new_date_format) if col.name in self.date_columns.values() else col
            )
        except Exception as e:
            print(f"Error converting date columns: {e}")

    # Map missing COUNTRY_CODE
    def map_country_codes(self):
        self.df['COUNTRY_CODE'] = self.df['COUNTRY_CODE'].str.strip().fillna(self.df['ROYALTY_CURRENCY'].map(self.currency_to_country_mapping))

    # Aggregate metric columns
    def aggregate_data(self, metric_col):
        try:
            def unique_join(series):
                return '|'.join(pd.Series.unique(series))
            agg_columns = {
                self.metric_columns[self.CUSTOMER_PRICE]: 'mean',
                self.metric_columns[self.ROYALTY_PRICE]: 'mean',
                self.metric_columns[self.UNITS]: 'sum',
                self.ASSET_CONTENT_FLAVOR: unique_join
            }


            self.df = self.df.groupby([self.VENDOR_IDENTIFIER, self.COUNTRY_CODE, self.BEGIN_DATE, 'NEW_TITLE', 
                                       self.PRIMARY_GENRE, self.CONVERSION_DATE]).agg(agg_columns).reset_index()
            return self.df
        except Exception as e:
            print(f"Error processing title: {e}")

    # Calculate Revenue and Cost
    def calculate_revenue_cost(self):
        self.df['REVENUE'] = self.df[self.metric_columns[self.CUSTOMER_PRICE]] * self.df[self.metric_columns[self.UNITS]]
        self.df['COST'] = self.df[self.metric_columns[self.ROYALTY_PRICE]] * self.df[self.metric_columns[self.UNITS]]

# Add vendor name column
    def add_vendor_name_column(self, vendor_name='ITunes'):
        try:
            self.df.insert(0, 'VENDOR_NAME', vendor_name)
            return self.df
        except Exception as e:
            print(f"Error adding vendor name column: {e}")

    # Call all functions
    def process_data_source(self):
        try:
            self.drop_columns()
            self.remove_special_characters_from_columns()
            self.process_new_title_and_drop_columns()
            self.convert_date_columns()
            self.map_country_codes()
            self.aggregate_data(self.metric_columns[self.CUSTOMER_PRICE])
            self.calculate_revenue_cost()
            return self.add_vendor_name_column()
        except Exception as e:
            print(f"Error processing data source: {e}")

