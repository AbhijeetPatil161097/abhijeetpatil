#!/usr/bin/env python
# coding: utf-8

# In[3]:


import numpy as np
import pandas as pd

class DtoDataProcessItunes:

    # Column name variables
    PROVIDER = 'Provider'
    PROVIDER_COUNTRY = 'Provider Country'
    VENDOR_IDENTIFIER = 'Vendor Identifier'
    UPC = 'UPC'
    ISRC = 'ISRC'
    ARTIST_SHOW = 'Artist / Show'
    TITLE = 'Title'
    LABEL_STUDIO_NETWORK = 'Label/Studio/Network'
    PRODUCT_TYPE_IDENTIFIER = 'Product Type Identifier'
    UNITS = 'Units'
    ROYALTY_PRICE = 'Royalty Price'
    BEGIN_DATE = 'Begin Date'
    END_DATE = 'End Date'
    CUSTOMER_CURRENCY = 'Customer Currency'
    COUNTRY_CODE = 'Country Code'
    ROYALTY_CURRENCY = 'Royalty Currency'
    PREORDER = 'PreOrder'
    ISAN = 'ISAN'
    APPLE_IDENTIFIER = 'Apple Identifier'
    CUSTOMER_PRICE = 'Customer Price'
    CMA = 'CMA'
    ASSET_CONTENT_FLAVOR = 'Asset/Content Flavor'
    VENDOR_OFFER_CODE = 'Vendor Offer Code'
    GRID = 'Grid'
    PROMO_CODE = 'Promo Code'
    PARENT_IDENTIFIER = 'Parent Identifier'
    PARENT_TYPE_ID = 'Parent Type Id'
    PRIMARY_GENRE = 'Primary Genre'
    EXTENDED_PARTNER_SHARE = 'Extended Partner Share'
    SALES_OR_RETURN = 'Sales or Return'
    REGION = 'Region'
    REVENUE_NATIVE = 'REVENUE_NATIVE'
    COST_NATIVE = 'COST_NATIVE'

    
    
    def __init__(self, platform, df):
        self.platform = platform
        self.df = df
        self.columns_to_drop = [self.PROVIDER, self.UPC, self.ISRC, self.LABEL_STUDIO_NETWORK, 
                                self.PRODUCT_TYPE_IDENTIFIER, self.CUSTOMER_CURRENCY, self.PREORDER, 
                                self.ISAN, self.APPLE_IDENTIFIER, self.CMA, self.VENDOR_OFFER_CODE, self.GRID, self.PROMO_CODE,
                                self.PARENT_IDENTIFIER, self.PARENT_TYPE_ID, self.EXTENDED_PARTNER_SHARE, self.REGION, self.END_DATE,
                                self.PROVIDER_COUNTRY]
        
        self.title_columns = [self.ARTIST_SHOW, self.TITLE]
        self.metric_columns = [self.CUSTOMER_PRICE, self.ROYALTY_PRICE, self.UNITS]
        self.df.dropna(subset=[self.VENDOR_IDENTIFIER, self.TITLE, self.COUNTRY_CODE], inplace=True)
        self.groupby_columns = [self.VENDOR_IDENTIFIER, self.COUNTRY_CODE, 'NEW_TITLE', self.BEGIN_DATE]
        self.currency_to_country_mapping = {'USD': 'US', 'CAD': 'CA', 'EUR': 'DE', 'GBP': 'GB', 'AUD': 'AU'}
        
        self.sales_return_mapping = {'S': 'SALES', 'R': 'RETURN'}
        
        self.df[self.BEGIN_DATE] = pd.to_datetime(self.df[self.BEGIN_DATE]).dt.strftime('%m-%Y')
        self.df[self.ASSET_CONTENT_FLAVOR].fillna('UNKNOWN', inplace=True)
        self.df[self.PRIMARY_GENRE].fillna('UNKNOWN', inplace=True)

        
        
        # Drop redundant columns
    def drop_columns(self):
        try:
            self.df.drop(columns=[col for col in self.columns_to_drop if col in self.df.columns], inplace=True)
        except Exception as e:
            raise RuntimeError(f"Error dropping columns: {e}")

    # Create NEW_TITLE column by concatenating title columns
    def new_title(self, row):
        try:
            season_title = str(row[self.ARTIST_SHOW]).replace('"', '').strip()
            title = str(row[self.TITLE]).replace('"', '').strip()

            if season_title in title:
                season_title_part = season_title[len(season_title):].strip()
                return f"{season_title_part} | {title}"
            else:
                return f"{season_title} | {title}"
        except Exception as e:
            raise RuntimeError(f"Error processing new title: {e}")

    # Add NEW_TITLE to df and delete old title columns
    def process_new_title_and_drop_columns(self):
        try:
            self.df['NEW_TITLE'] = self.df.apply(self.new_title, axis=1)
            self.df['NEW_TITLE'] = self.df['NEW_TITLE'].fillna('').apply(lambda x: x.replace('|', '').strip())
            for column in self.title_columns:
                if column in self.df.columns:
                    self.df = self.df.drop(column, axis=1)
        except Exception as e:
            raise RuntimeError(f"Error processing title and dropping columns: {e}")

    # Map Missing country codes using currency
    def map_country_codes(self):
        try:
            self.df[self.COUNTRY_CODE] = self.df[self.COUNTRY_CODE].str.strip().fillna(
                self.df[self.ROYALTY_CURRENCY].map(self.currency_to_country_mapping)
            )
            self.df.drop(columns=[self.ROYALTY_CURRENCY], inplace=True)
        except Exception as e:
            raise RuntimeError(f"Error mapping country codes: {e}")
            
    # Calculate revenue and cost       
    def calculate_revenue_cost(self):
        try:
            self.df['REVENUE_NATIVE'] = self.df[self.CUSTOMER_PRICE] * self.df[self.UNITS]
            self.df['COST_NATIVE'] = self.df[self.ROYALTY_PRICE] * self.df[self.UNITS] 
        except Exception as e:
            raise RuntimeError(f"Error calculating revenue and cost: {e}")

            
    # Rename S =SALES, R=RETURN 
    def map_sales_return(self):
        try:
            self.df[self.SALES_OR_RETURN] = self.df[self.SALES_OR_RETURN].map(self.sales_return_mapping)
            self.df[self.SALES_OR_RETURN].fillna('UNKNOWN', inplace=True)
        except Exception as e:
            raise RuntimeError(f"Error mapping sales/return: {e}")

    # Aggregate rows    
    def aggregate_data(self):
        try:
            def unique_join(series):
                return '|'.join(sorted(pd.Series.unique(series)))

            agg_columns = {
                self.CUSTOMER_PRICE: 'mean',
                self.ROYALTY_PRICE: 'mean',
                self.UNITS: 'sum',
                self.REVENUE_NATIVE: 'sum',
                self.COST_NATIVE: 'sum',
                self.SALES_OR_RETURN: unique_join,
                self.PRIMARY_GENRE: unique_join,
                self.ASSET_CONTENT_FLAVOR: unique_join
            }

            self.df = self.df.groupby(self.groupby_columns).agg(agg_columns).reset_index()
        except Exception as e:
            raise RuntimeError(f"Error aggregating data: {e}")


    # Format column names       
    def rename_columns(self):
        try:
            self.df = self.df.rename(columns=lambda x: x.upper().replace(' ', '_'))
        except Exception as e:
            raise RuntimeError(f"Error renaming columns: {e}")

    # Add ITUNES as vendor name
    def add_vendor_name_column(self, vendor_name='ITUNES'):
        try:
            self.df.insert(0, 'VENDOR_NAME', vendor_name)
            return self.df
        except Exception as e:
            raise RuntimeError(f"Error adding vendor name column: {e}")

    # Create a function to call all above functions
    def process_data_source(self):
        try:
            self.drop_columns()
            self.process_new_title_and_drop_columns()
            self.map_country_codes()
            self.map_sales_return()
            self.calculate_revenue_cost()
            self.aggregate_data()
            self.rename_columns()
            return self.add_vendor_name_column()
        except RuntimeError as e:
            raise e
# In[ ]:




