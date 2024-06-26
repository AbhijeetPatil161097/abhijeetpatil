#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pandas as pd

def merge_dataframes(df_amazon, df_itunes, df_google):
    # Check if any DataFrame is None
    if df_amazon is None and df_itunes is None and df_google is None:
        raise ValueError("All input DataFrames are None")
    
    # Initialize empty list to store non-None DataFrames
    non_none_dfs = []
    
    # Rename and append non-None DataFrames
    if df_amazon is not None:
        df_amazon = df_amazon.rename(columns={
            'SKU_NUMBER': 'VENDOR_ASSET_ID', 
            'NEW_TITLE': 'TITLE', 
            'Transaction': 'TRANSACTION_TYPE'
        })
        
        non_none_dfs.append(df_amazon)
    
    if df_itunes is not None:
        df_itunes = df_itunes.rename(columns={
            'VENDOR_IDENTIFIER': 'VENDOR_ASSET_ID', 
            'NEW_TITLE': 'TITLE', 
            'UNITS': 'QUANTITY', 
            'ASSET/CONTENT_FLAVOR': 'MEDIA_FORMAT',
            'COUNTRY_OF_SALE': 'TERRITORY', 
            'START_DATE': 'TRANSACTION_DATE', 
            'SALES_OR_RETURN' : 'TRANSACTION_TYPE'
        })
        
        non_none_dfs.append(df_itunes)
    
    if df_google is not None:
        df_google = df_google.rename(columns={
            'YOUTUBE_VIDEO_ID' : 'VENDOR_ASSET_ID', 
            'NEW_TITLE' : 'TITLE',
            'RESOLUTION' : 'MEDIA_FORMAT', 
            'COUNTRY': 'TERRITORY',
        })
        
        non_none_dfs.append(df_google)
    
    # Check if no non-None DataFrames were found
    if len(non_none_dfs) == 0:
        raise ValueError("All input DataFrames are empty")
    
    # Concatenate non-None DataFrames
    df_combined = pd.concat(non_none_dfs, ignore_index=True)
    return df_combined



# In[ ]:




