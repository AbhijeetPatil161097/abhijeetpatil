#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd

def merge_dataframes(df1, df2, df3):
    # Rename columns
    df1.rename(columns={'SKU_NUMBER': 'VENDOR_ASSET_ID', 'MONTH_YEAR': 'TRANSACTION_DATE', 'NEW_TITLE': 'TITLE'}, inplace=True)
    df2.rename(columns={'VENDOR_IDENTIFIER': 'VENDOR_ASSET_ID', 'NEW_TITLE': 'TITLE', 'ROYALTY_PRICE': 'RETAIL_PRICE',
                        'CUSTOMER_PRICE': 'UNIT_COST', 'UNITS': 'QUANTITY', 'ASSET_CONTENT_FLAVOR': 'MEDIA_FORMAT',
                        'COUNTRY_CODE': 'TERRITORY', 'BEGIN_DATE': 'TRANSACTION_DATE'}, inplace=True)
    df3.rename(columns={'YOUTUBE_VIDEO_ID':'VENDOR_ASSET_ID', 'NEW_TITLE':'TITLE', 'RETAIL_PRICE_USD':'RETAIL_PRICE',
                       'RESOLUTION':'MEDIA_FORMAT', 'COUNTRY':'TERRITORY'}, inplace=True)
    
    

    # Concatenate dataframes
    final_df = pd.concat([df1, df2, df3], ignore_index=True)
    return final_df
## test1
