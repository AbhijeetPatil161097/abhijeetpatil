#!/usr/bin/env python
# coding: utf-8

# In[ ]:

## 123##
class DtoDataProcessAmazon:
    # Define column names as class variables
    FILE_NAME = 'FILE_NAME'
    HEADER = 'HEADER'
    FOOTER = 'FOOTER'
    MONTH = 'MONTH'
    YEAR = 'YEAR'
    DISC_PLUS = 'DISC_PLUS'
    TRANSACTION = 'TRANSACTION'
    CATEGORY = 'CATEGORY'
    TRANSACTION_FORMAT = 'TRANSACTION_FORMAT'
    MEDIA_FORMAT = 'MEDIA_FORMAT'
    SKU_NUMBER = 'SKU_NUMBER'
    TITLE = 'TITLE'
    SEASON_TITLE = 'SEASON_TITLE'
    SERIES_TITLE = 'SERIES_TITLE'
    DVD_STREET_DATE = 'DVD_STREET_DATE'
    THEATRICAL_RELEASE_DATE = 'THEATRICAL_RELEASE_DATE'
    RETAIL_PRICE = 'RETAIL_PRICE'
    UNIT_COST = 'UNIT_COST'
    EPISODE_NUMBER = 'EPISODE_NUMBER'
    CYS_DISCOUNT = 'CYS_DISCOUNT'
    CYS_EPISODE_COUNT = 'CYS_EPISODE_COUNT'
    QUANTITY = 'QUANTITY'
    REVENUE = 'REVENUE'
    COST = 'COST'
    VENDOR_CODE = 'VENDOR_CODE'
    TERRITORY = 'TERRITORY'
    CONVERSION_DATE = 'CONVERSION_DATE'

    def __init__(self, platform, df):
        self.platform = platform
        self.df = df
        
        # Define columns to drop
        self.columns_to_drop = [
            self.FILE_NAME, self.HEADER, self.FOOTER, self.DVD_STREET_DATE,
            self.THEATRICAL_RELEASE_DATE, self.EPISODE_NUMBER, self.VENDOR_CODE,
            self.DISC_PLUS, self.CYS_EPISODE_COUNT
        ]
        
        # Define groupby columns using variables directly
        self.groupby_columns = [
            self.SKU_NUMBER, self.TERRITORY, 'MONTH_YEAR', 'NEW_TITLE', self.TRANSACTION_FORMAT, 
        ]
        
        # Define metric columns
        self.metric_columns =[self.RETAIL_PRICE, self.UNIT_COST, self.QUANTITY, self.REVENUE, self.COST]
        for item in self.metric_columns:
            self.df[item] = pd.to_numeric(self.df[item], errors='coerce')

        self.df.fillna(value={col: 0 for col in self.metric_columns}, inplace=True)

    # Drop Redundant Columns
    def drop_columns(self, columns_to_drop=None):
        try:
            columns_to_drop = columns_to_drop or self.columns_to_drop
            existing_columns = [col for col in columns_to_drop if col in self.df.columns]
            self.df.drop(columns=existing_columns, inplace=True)
        except KeyError as e:
            print(f"Error dropping columns: {e}")
        except Exception as e:
            print(f"Unexpected error dropping columns: {e}")

    # Create new title by adding all title columns
    def new_title(self, row):
        try:
            series_title = str(row[self.SERIES_TITLE]).replace('"', '').strip()
            season_title = str(row[self.SEASON_TITLE]).replace('"', '').strip()
            title = str(row[self.TITLE]).replace('"', '').strip()

            if series_title and season_title:
                if series_title in season_title:
                    season_part = season_title[len(series_title):].strip()
                    return f"{series_title} | {season_part} | {title}"

            return f"{series_title} {title}"
        except KeyError as e:
            print(f"Error processing title: {e}")
        except Exception as e:
            print(f"Unexpected error processing title: {e}")

    # Add new title to df and drop other title columns
    def process_new_title_and_drop_columns(self):
        try:
            # Add 'NEW_TITLE' column
            self.df['NEW_TITLE'] = self.df.apply(lambda row: self.new_title(row), axis=1)

            # Drop unnecessary columns
            self.df.drop(columns=[self.SERIES_TITLE, self.SEASON_TITLE, self.TITLE], inplace=True)
        except KeyError as e:
            print(f"Error dropping old title columns: {e}")
        except Exception as e:
            print(f"Unexpected error processing new title and dropping columns: {e}")

    # Replace New titles by most common English titles using similar SKUs
    def replace_titles(self, territory_filter='US', sku_number_filter=' ', title_column='NEW_TITLE'):
        try:
            top_english_titles = self.df[(self.df['TERRITORY'] == territory_filter) & (self.df['SKU_NUMBER'] != sku_number_filter)]
            most_frequent_titles = top_english_titles.groupby('SKU_NUMBER')[title_column].apply(lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else x.iloc[0]).reset_index(name=title_column)
            self.df = pd.merge(self.df, most_frequent_titles, on='SKU_NUMBER', how='left')
            self.df[title_column] = self.df[f'{title_column}_y'].combine_first(self.df[f'{title_column}_x'])
            self.df.drop(columns=[f'{title_column}_x', f'{title_column}_y'], inplace=True)
            self.df[title_column] = self.df[title_column].str.strip()
        except KeyError as e:
            print(f"Error replacing titles: {e}")
        except Exception as e:
            print(f"Unexpected error replacing titles: {e}")

    # Remove Quotations from SKUs
    def remove_quotations(self):
        try:
            self.df[self.SKU_NUMBER] = self.df[self.SKU_NUMBER].str.replace('"', '')
        except KeyError as e:
            print(f"Error removing quotations: {e}")
        except Exception as e:
            print(f"Unexpected error removing quotations: {e}")

    # Combine MONTH and YEAR column.
    def month_year(self):
        try:
            self.df['MONTH_YEAR'] = pd.to_datetime(self.df[self.MONTH].astype(str) + '-' + self.df[self.YEAR].astype(str), format='%m-%Y')
            self.df.drop(columns=[self.MONTH, self.YEAR], inplace=True)
        except KeyError as e:
            print(f"Error combining month and year: {e}")
        except Exception as e:
            print(f"Unexpected error combining month and year: {e}")

    # Format conversion date as in date format
    def formatting_conversion_date(self):
        try:
            self.df[self.CONVERSION_DATE] = pd.to_datetime(self.df[self.CONVERSION_DATE]).dt.date
        except KeyError as e:
            print(f"Error formatting conversion date: {e}")
        except Exception as e:
            print(f"Unexpected error formatting conversion date: {e}")

    # Calculate missing revenue values by ( RETAIL_PRICE X QUANTITY)
    def calculate_revenue(self):
        try:
            self.df[self.REVENUE] = self.df[self.RETAIL_PRICE] * self.df[self.QUANTITY].abs()
        except KeyError as e:
            print(f"Error calculating revenue: {e}")
        except Exception as e:
            print(f"Unexpected error calculating revenue: {e}")

    # Aggregate rows by similar column values.
    def aggregate_data(self):
        try:
            def unique_join(series):
                return '|'.join(pd.Series.unique(series))
            agg_columns = {
                self.RETAIL_PRICE: 'mean', 
                self.UNIT_COST: 'mean',
                self.QUANTITY: 'sum', 
                self.REVENUE: 'sum', 
                self.COST: 'sum',
                'MEDIA_FORMAT': unique_join
            }

            self.df = self.df.groupby(self.groupby_columns).agg(agg_columns).reset_index()
        except KeyError as e:
            print(f"Error aggregating data: {e}")
        except Exception as e:
            print(f"Unexpected error aggregating data: {e}")

    # Add AMAZON as vendor name
    def add_vendor_name_column(self):
        try:
            self.df.insert(0, 'VENDOR_NAME', 'AMAZON')
            return self.df
        except KeyError as e:
            print(f"Error adding vendor name column: {e}")
            return self.df
        except Exception as e:
            print(f"Unexpected error adding vendor name column: {e}")
            return self.df

    # Call all functions
    def process_data_source(self):
        try: 
            self.drop_columns()
            self.process_new_title_and_drop_columns()
            self.replace_titles()
            self.remove_quotations()
            self.month_year()
            self.formatting_conversion_date()
            self.calculate_revenue()
            self.aggregate_data()
            return self.add_vendor_name_column()
        except Exception as e:
            print(f"Error processing data source: {e}")


# In[ ]:




