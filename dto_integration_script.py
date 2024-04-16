#!/usr/bin/env python
# coding: utf-8

# In[3]:


def merge_dataframes(df1, df2, df3):
    # Rename columns
    df_amazon = df1.rename(columns={'SKU_NUMBER': 'VENDOR_ASSET_ID', 'NEW_TITLE': 'TITLE', 'RETAIL_PRICE' : 'RETAIL_PRICE_NATIVE',
                       'UNIT_COST' : 'UNIT_COST_NATIVE', 'REVENUE' : 'REVENUE_NATIVE', 'COST' : 'COST_NATIVE'})
    
    df_itunes = df2.rename(columns={'VENDOR_IDENTIFIER': 'VENDOR_ASSET_ID', 'NEW_TITLE': 'TITLE', 'ROYALTY_PRICE': 'RETAIL_PRICE_NATIVE',
                        'CUSTOMER_PRICE': 'UNIT_COST_NATIVE', 'UNITS': 'QUANTITY', 'ASSET/CONTENT_FLAVOR': 'MEDIA_FORMAT',
                        'COUNTRY_CODE': 'TERRITORY', 'BEGIN_DATE': 'TRANSACTION_DATE', 'SALES_OR_RETURN' : 'TRANSACTION_FORMAT'})
    
    df_google = df3.rename(columns={'YOUTUBE_VIDEO_ID' : 'VENDOR_ASSET_ID', 'RESOLUTION' : 'MEDIA_FORMAT', 'COUNTRY': 'TERRITORY', 
                        'NEW_TITLE' : 'TITLE', 'RETAIL_PRICE' : 'RETAIL_PRICE_USD', 'NATIVE_RETAIL_PRICE' : 'RETAIL_PRICE_NATIVE'})
    
    # Concatenate dataframes
    df_combined = pd.concat([df_amazon, df_itunes, df_google], ignore_index=True)
    return df_combined
# 123

# In[ ]:




