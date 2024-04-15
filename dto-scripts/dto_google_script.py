#!/usr/bin/env python
# coding: utf-8

# In[3]:


class DtoDataProcessGoogle:
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
    NATIVE_RETAIL_PRICE = 'Native Retail Price'
    TAX_EXCLUSIVE_RETAIL_PRICE_USD = 'Tax Exclusive Retail Price'
    NATIVE_TAX_EXCLUSIVE_RETAIL_PRICE = 'Native Tax Exclusive Retail Price'
    TOTAL_TAX = 'Total Tax'
    NATIVE_TOTAL_TAX = 'Native Total Tax'
    PARTNER_EARNINGS_FRACTION = 'Partner Earnings Fraction'
    CONTRACTUAL_MINIMUM_PARTNER_EARNINGS_USD = 'Contractual Minimum Partner Earnings'
    NATIVE_CONTRACTUAL_MINIMUM_PARTNER_EARNINGS = 'Native Contractual Minimum Partner Earnings'
    PARTNER_EARNINGS_USING_TAX_EXCLUSIVE_RETAIL_PRICE_USD = 'Partner Earnings Using Tax Exclusive Retail Price'
    NATIVE_PARTNER_EARNINGS_USING_TAX_EXCLUSIVE_RETAIL_PRICE = 'Native Partner Earnings Using Tax Exclusive Retail Price'
    PARTNER_FUNDED_DISCOUNTS_USD = 'Partner Funded Discounts'
    NATIVE_PARTNER_FUNDED_DISCOUNTS = 'Native Partner Funded Discounts'
    FINAL_PARTNER_EARNINGS_USD = 'Final Partner Earnings'
    NATIVE_FINAL_PARTNER_EARNINGS = 'Native Final Partner Earnings'
    CONVERSION_RATE = 'Conversion Rate'
    NATIVE_RETAIL_CURRENCY = 'Native Retail Currency'
    NATIVE_PARTNER_CURRENCY = 'Native Partner Currency'
    QUANTITY = 'QUANTITY'
    
    def __init__(self, platform, df):
        self.platform = platform
        self.df = df.copy()
        self.df['QUANTITY'] = 1
        
        self.columns_to_drop = [self.EIDR_TITLE_ID, self.EIDR_EDIT_ID, self.TRANSACTION_IMPORT_SOURCE, self.TAX_EXCLUSIVE_RETAIL_PRICE_USD,
            self.REFUND_CHARGEBACK, self.COUPON_USED, self.CAMPAIGN_ID, self.TOTAL_TAX, self.NATIVE_TAX_EXCLUSIVE_RETAIL_PRICE,
            self.NATIVE_CONTRACTUAL_MINIMUM_PARTNER_EARNINGS, self.NATIVE_TOTAL_TAX,
            self.NATIVE_PARTNER_EARNINGS_USING_TAX_EXCLUSIVE_RETAIL_PRICE, self.PARTNER_EARNINGS_FRACTION, 
            self.NATIVE_PARTNER_FUNDED_DISCOUNTS, self.NATIVE_FINAL_PARTNER_EARNINGS, self.CONTRACTUAL_MINIMUM_PARTNER_EARNINGS_USD,
            self.CONTRACTUAL_MINIMUM_PARTNER_EARNINGS_USD, self.PARTNER_FUNDED_DISCOUNTS_USD,
            self.NATIVE_RETAIL_CURRENCY, self.NATIVE_PARTNER_CURRENCY]


                                
        self.title_columns = [self.SHOW_TITLE, self.SEASON_TITLE, self.NAME_OF_TITLE]
        self.groupby_columns = [self.COUNTRY, 'NEW_TITLE', self.YOUTUBE_VIDEO_ID, self.TRANSACTION_DATE]
        self.metric_columns = [self.RETAIL_PRICE_USD, self.NATIVE_RETAIL_PRICE, self.QUANTITY, self.CONVERSION_RATE]
        self.df[self.metric_columns] = self.df[self.metric_columns].apply(pd.to_numeric)
        
    
    
    # Drop null rows
    def drop_null_rows(self):
        try:
            self.df = self.df.dropna(subset = [self.TRANSACTION_DATE, self.YOUTUBE_VIDEO_ID, self.CONVERSION_RATE])
            return self.df
        except Exception as e:
            print(f"Error dropping rows: {e}")
            
    
    # Format transaction Dates
    def format_transaction_dates(self):
        try:
            self.df[self.TRANSACTION_DATE] = pd.to_datetime(self.df[self.TRANSACTION_DATE])
            self.df[self.TRANSACTION_DATE] = self.df[self.TRANSACTION_DATE].dt.strftime('%m-%Y')
            return self.df
        
        except Exception as e:
            print(f"Error Formatting Transaction Dates: {e}")
    
    
    # Drop redundantt columns
    def drop_columns(self, columns_to_drop=None):
        try:
            columns_to_drop = columns_to_drop or self.columns_to_drop
            existing_columns = [col for col in columns_to_drop if col in self.df.columns]
            self.df.drop(columns=existing_columns, inplace=True)
            return self.df
        except Exception as e:
            print(f"Error dropping columns: {e}")
    
    
    # Create NEW_TITLE using title columns
    def new_title(self, row):
        try:
            series_title = str(row[self.SHOW_TITLE]).replace('"', '').strip() if not pd.isna(row[self.SHOW_TITLE]) else ''
            season_title = str(row[self.SEASON_TITLE]).replace('"', '').strip() if not pd.isna(row[self.SEASON_TITLE]) else ''
            title = str(row[self.NAME_OF_TITLE]).replace('"', '').strip() if not pd.isna(row[self.NAME_OF_TITLE]) else ''
        
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
            print(f"Error processing title: {e}")     
            
            
    
    # Add NEW_TITLE to df and drop old title
    def process_new_title_and_drop_columns(self):
        try:
            self.df['NEW_TITLE'] = self.df.apply(lambda row: self.new_title(row), axis=1)
            self.df['NEW_TITLE'] = self.df['NEW_TITLE'].fillna('').apply(lambda x: x.replace('|', '').strip())
            self.df.drop(columns=[self.SHOW_TITLE, self.SEASON_TITLE, self.NAME_OF_TITLE], inplace=True)
            return self.df
        except KeyError as e:
            print(f"Error dropping old title columns: {e}")

    
    
    # Aggregate rows
    def aggregate_data(self):
        try:
            def unique_join(series):
                return '|'.join(sorted(map(str, pd.Series.unique(series))))
            agg_columns = {
                self.RETAIL_PRICE_USD : 'mean',
                self.NATIVE_RETAIL_PRICE : 'mean',
                self.QUANTITY : 'sum',
                self.CONVERSION_RATE:'mean',
                self.RESOLUTION : unique_join,
                self.TRANSACTION_TYPE : unique_join,
                self.PURCHASE_LOCATION : unique_join,
                self.VIDEO_CATEGORY : unique_join
            }

            self.df = self.df.groupby(self.groupby_columns).agg(agg_columns).reset_index()
        except KeyError as e:
            print(f"Error aggregating data: {e}")
            
    
    # Calculate revenue
    def calculate_revenue(self):
        try:
            self.df['REVENUE_USD'] = self.df[self.RETAIL_PRICE_USD] * self.df[self.QUANTITY]
            self.df['REVENUE_NATIVE'] = self.df[self.NATIVE_RETAIL_PRICE] * self.df[self.QUANTITY]
        except Exception as e:
            print(f"Error calculation revenue: {e}")
            
    
    # Format column names
    def rename_columns(self):
        try:
            self.df = self.df.rename(columns=lambda x: x.upper().replace(' ', '_'))
            return self.df
        except Exception as e:  
            print(f"Error adding renaming column names: {e}")
            return self.df
    
    
    # Add GOOGLE as Vendor name
    def add_vendor_name_column(self, vendor_name='GOOGLE'):
        try:
            self.df.insert(0, 'VENDOR_NAME', vendor_name)
            return self.df
        except Exception as e:
            print(f"Error adding vendor name column: {e}")
            return self.df
        
        
    
    # Add function to call all         
    def process_data_source(self):
        try:
            self.drop_null_rows()
            self.drop_columns()
            self.format_transaction_dates()
            self.process_new_title_and_drop_columns()
            self.aggregate_data()
            self.calculate_revenue()
            self.rename_columns()
            return self.add_vendor_name_column()
        except Exception as e:
            print(f"Error processing data source: {e}")


# In[ ]:

#rrr


