import os
import pandas as pd

def generate_daily_csvs(data_path):
        os.makedirs('logs/daily_sales', exist_ok=True)
        df = pd.read_csv(data_path, encoding='latin-1')
        df['order_date'] = df['order_date'].astype('datetime64[ns]')
        df['ship_date'] = df['ship_date'].astype('datetime64[ns]')
        df['year'], df['month'], df['day'] = df['order_date'].dt.year, df['order_date'].dt.month, df['order_date'].dt.day
        for date in df['order_date'].unique():
            daily_df = df[df['order_date'] == date]
            filename = f"logs/daily_sales/sales_{date.strftime('%Y-%m-%d')}.csv"
            daily_df.to_csv(filename, index=False)

def aggregate_country_sales_by_date(data_path, country):
    data = pd.read_csv(data_path, encoding='latin-1')
    data['sales'] = data['sales'].str.replace(',', '').astype(float)
    country_data = data[data['country'] == f'{country}']
    country_sales_by_date = country_data.groupby('order_date')['sales'].sum().reset_index()
    country_sales_by_date['order_date'] = country_sales_by_date['order_date'].astype('datetime64[ns]')
    country_sales_by_date = country_sales_by_date.sort_values('order_date')
    os.makedirs('data/processed', exist_ok=True)
    country_sales_by_date.to_csv(f'data/processed/{country}_sales_by_date.csv', index=False)