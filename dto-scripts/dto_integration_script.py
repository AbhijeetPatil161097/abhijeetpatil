#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pandas as pd

def merge_dataframes(df1, df2, df3):
    # Check if any DataFrame is None
    if df1 is None and df2 is None and df3 is None:
        raise ValueError("All input DataFrames are None")
    
    # Initialize empty list to store non-None DataFrames
    non_none_dfs = []
    
    # Rename and append non-None DataFrames
    if df1 is not None:
        df_amazon = df1.rename(columns={'SKU_NUMBER': 'VENDOR_ASSET_ID', 'NEW_TITLE': 'TITLE', 'RETAIL_PRICE' : 'RETAIL_PRICE_NATIVE',
                           'UNIT_COST' : 'UNIT_COST_NATIVE', 'REVENUE' : 'REVENUE_NATIVE', 'COST' : 'COST_NATIVE'})
        non_none_dfs.append(df_amazon)
    
    if df2 is not None:
        df_itunes = df2.rename(columns={'VENDOR_IDENTIFIER': 'VENDOR_ASSET_ID', 'NEW_TITLE': 'TITLE', 'ROYALTY_PRICE': 'RETAIL_PRICE_NATIVE',
                            'CUSTOMER_PRICE': 'UNIT_COST_NATIVE', 'UNITS': 'QUANTITY', 'ASSET/CONTENT_FLAVOR': 'MEDIA_FORMAT',
                            'PROVIDER_COUNTRY': 'TERRITORY', 'BEGIN_DATE': 'TRANSACTION_DATE', 'SALES_OR_RETURN' : 'TRANSACTION_FORMAT'})
        non_none_dfs.append(df_itunes)
    
    if df3 is not None:
        df_google = df3.rename(columns={'YOUTUBE_VIDEO_ID' : 'VENDOR_ASSET_ID', 'RESOLUTION' : 'MEDIA_FORMAT', 'COUNTRY': 'TERRITORY', 
                            'NEW_TITLE' : 'TITLE', 'RETAIL_PRICE' : 'RETAIL_PRICE_USD', 'NATIVE_RETAIL_PRICE' : 'RETAIL_PRICE_NATIVE'})
        non_none_dfs.append(df_google)
    
    # Check if no non-None DataFrames were found
    if len(non_none_dfs) == 0:
        raise ValueError("All input DataFrames are None")
    
    # Concatenate non-None DataFrames
    df_combined = pd.concat(non_none_dfs, ignore_index=True)
    return df_combined



# In[ ]:




