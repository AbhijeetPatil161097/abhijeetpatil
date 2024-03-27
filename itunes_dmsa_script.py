#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import re

class DtoDataProcessItunes:

    # Column name variables
    START_DATE = 'Start Date'
    END_DATE = 'End Date'
    UPC = 'UPC'
    ISRC_ISBN = 'ISRC/ISBN'
    VENDOR_IDENTIFIER = 'Vendor Identifier'
    QUANTITY = 'Quantity'
    PARTNER_SHARE = 'Partner Share'
    EXTENDED_PARTNER_SHARE = 'Extended Partner Share'
    PARTNER_SHARE_CURRENCY = 'Partner Share Currency'
    SALES_OR_RETURN = 'Sales or Return'
    APPLE_IDENTIFIER = 'Apple Identifier'
    ARTIST_SHOW_DEVELOPER_AUTHOR = 'Artist/Show/Developer/Author'
    TITLE = 'Title'
    LABEL_STUDIO_NETWORK_DEVELOPER_PUBLISHER = 'Label/Studio/Network/Developer/Publisher'
    GRID = 'Grid'
    PRODUCT_TYPE_IDENTIFIER = 'Product Type Identifier'
    ISAN_OTHER_IDENTIFIER = 'ISAN/Other Identifier'
    COUNTRY_OF_SALE = 'Country Of Sale'
    PREORDER_FLAG = 'Pre-order Flag'
    PROMO_CODE = 'Promo Code'
    CUSTOMER_PRICE = 'Customer Price'
    CUSTOMER_CURRENCY = 'Customer Currency'
    REGION = 'Region'


    def __init__(self, platform, df):
        self.platform = platform
        self.df = df
        self.columns_to_drop = [self.UPC, self.ISRC_ISBN, self.APPLE_IDENTIFIER, self.PROMO_CODE, 
                                self.ISAN_OTHER_IDENTIFIER, self.LABEL_STUDIO_NETWORK_DEVELOPER_PUBLISHER]
        self.title_columns = [self.ARTIST_SHOW_DEVELOPER_AUTHOR, self.TITLE]
        self.metric_columns = [self.CUSTOMER_PRICE, self.PARTNER_SHARE, self.EXTENDED_PARTNER_SHARE, self.QUANTITY]
        self.date_columns = [self.START_DATE, self.END_DATE]
        self.currency_to_country_mapping = {'USD': 'US', 'CAD': 'CA', 'EUR': 'DE', 'GBP': 'GB', 'AUD': 'AU'}

    # Drop redundant columns
    def drop_columns(self):
        try:
            existing_columns = [col for col in self.columns_to_drop if col in self.df.columns]
            self.df.drop(columns=existing_columns, inplace=True)
        except Exception as e:
            print(f"Error dropping columns: {e}")


    # Combine Title attributes and create a single title column
    def new_title(self, row):
        try:
            series_title = str(row[self.ARTIST_SHOW_DEVELOPER_AUTHOR]).replace('"', '').strip() if pd.notnull(row[self.ARTIST_SHOW_DEVELOPER_AUTHOR]) else ''
            season_title = str(row[self.TITLE]).replace('"', '').strip() if pd.notnull(row[self.TITLE]) else ''

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

    # Map missing COUNTRY_OF_SALE
    def map_country_codes(self):
        self.df[self.COUNTRY_OF_SALE] = self.df[self.COUNTRY_OF_SALE].str.strip().fillna(
            self.df[self.PARTNER_SHARE_CURRENCY].map(self.currency_to_country_mapping))

    # Aggregate metric columns
    def aggregate_data(self):
        try:
            def unique_join(series):
                cleaned_values = [str(val) for val in pd.Series.unique(series) if pd.notnull(val)]
                return '|'.join(cleaned_values)

            agg_columns = {
                self.CUSTOMER_PRICE: 'mean',
                self.PARTNER_SHARE: 'mean',
                self.EXTENDED_PARTNER_SHARE: 'mean',
                self.QUANTITY: 'sum',
                self.REGION: unique_join
            }

            groupby_columns = [self.VENDOR_IDENTIFIER, self.COUNTRY_OF_SALE, self.START_DATE, 'NEW_TITLE']

            self.df = self.df.groupby(groupby_columns).agg(agg_columns).reset_index()
            return self.df
        except Exception as e:
            print(f"Error Aggregating Data: {e}")

    # Calculate Revenue and Cost
    def calculate_revenue_cost(self):
        self.df['REVENUE'] = self.df[self.CUSTOMER_PRICE] * self.df[self.QUANTITY]
        self.df['COST'] = self.df[self.PARTNER_SHARE] * self.df[self.QUANTITY]

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
            self.process_new_title_and_drop_columns()
            self.map_country_codes()
            self.aggregate_data()
            self.calculate_revenue_cost()
            return self.add_vendor_name_column()
        except Exception as e:
            print(f"Error processing data source: {e}")

