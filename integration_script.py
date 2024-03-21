#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd

def merge_dataframes(df1, df2):
    # Rename columns
    df1.rename(columns={'SKU_NUMBER': 'VENDOR_ASSET_ID', 'MONTH_YEAR': 'TRANSACTION_DATE', 'NEW_TITLE': 'TITLE'}, inplace=True)
    df2.rename(columns={'VENDOR_IDENTIFIER': 'VENDOR_ASSET_ID', 'NEW_TITLE': 'TITLE', 'ROYALTY_PRICE': 'RETAIL_PRICE',
                        'CUSTOMER_PRICE': 'UNIT_COST', 'UNITS': 'QUANTITY', 'ASSET_CONTENT_FLAVOR': 'MEDIA_FORMAT',
                        'COUNTRY_CODE': 'TERRITORY', 'BEGIN_DATE': 'TRANSACTION_DATE'}, inplace=True)
    # v1v2v3
    # Concatenate dataframes
    df_combined = pd.concat([df1, df2], ignore_index=True)
    df_combined['TRANSACTION_DATE'] = pd.to_datetime(df_combined['TRANSACTION_DATE'])
    df_combined['TRANSACTION_DATE'] = df_combined['TRANSACTION_DATE'].dt.strftime('%B %Y')
    return df_combined

