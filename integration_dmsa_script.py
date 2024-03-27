#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pandas as pd

def merge_dataframes(df1, df2, df3):
    # Rename columns
    df1.rename(columns={'SKU Number': 'VENDOR_ASSET_ID', 'MONTH_YEAR': 'TRANSACTION_DATE', 'NEW_TITLE': 'TITLE', 
                       'Territory' : 'TERRITORY', 'Transaction Format' : 'TRANSACTION_FORMAT', 'Retail Price' : 'RETAIL_PRICE_USD',
                       'Unit Cost' : 'UNIT_COST', 'Quantity' : 'QUANTITY', 'Revenue' : 'REVENUE', 'cost' : 'COST', 
                        'Media Format' : 'MEDIA_FORMAT'}, inplace=True)
    df2.rename(columns={'Vendor Identifier': 'VENDOR_ASSET_ID', 'NEW_TITLE': 'TITLE', 'Partner Share': 'RETAIL_PRICE',
                        'Customer Price': 'UNIT_COST', 'Quantity': 'QUANTITY', 'ASSET_CONTENT_FLAVOR': 'MEDIA_FORMAT',
                        'Country Of Sale': 'TERRITORY', 'Start Date': 'TRANSACTION_DATE'}, inplace=True)
    df3.rename(columns={'YouTube Video ID' : 'VENDOR_ASSET_ID', 'Resolution' : 'MEDIA_FORMAT', 'Country': 'TERRITORY', 
                        'NEW_TITLE' : 'TITLE', 'Retail Price' : 'RETAIL_PRICE_USD', 'Transaction Date' : 'TRANSACTION_DATE',
                       'Video Category' : 'VIDEO_CATEGORY', 'Purchase Location' : 'PURCHASE_LOCATION', 'Channel Name' : 'CHANNEL_NAME'
                       })
    
    # Concatenate dataframes
    df_combined = pd.concat([df1, df2, df3], ignore_index=True)
    df_combined['TRANSACTION_DATE'] = pd.to_datetime(df_combined['TRANSACTION_DATE'])
    df_combined['TRANSACTION_DATE'] = df_combined['TRANSACTION_DATE'].dt.strftime('%m %Y')
    return df_combined


# In[ ]:




