import pandas as pd
import numpy as np
import os

# Use the script directory so the CSV is located relative to this file
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV = os.path.join(BASE_DIR, 'customer_data_cache.csv')

def main():
    try:
        df = pd.read_csv(CSV)
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    except Exception as e:
        print(f"Failed to load {CSV}: {e}")
        return

    df = df[df['Price'].notna()] if 'Price' in df.columns else df

    unique_customers = df['Customer_ID'].astype(str).str.strip().nunique()

    df['Visit_Date'] = df['Date'].dt.date
    customer_visit_days = df.groupby('Customer_ID')['Visit_Date'].nunique()
    one_timers = (customer_visit_days == 1).sum()
    repeat_customers = (customer_visit_days > 1).sum()
    repeat_pct = (repeat_customers / unique_customers * 100) if unique_customers > 0 else 0

    customer_trx_counts = df.groupby('Customer_ID').size()
    repeat_trx_count = (customer_trx_counts > 1).sum()
    repeat_pct_trx = (repeat_trx_count / unique_customers * 100) if unique_customers > 0 else 0

    lifespan_df = df.groupby('Customer_ID')['Date'].agg(['min', 'max'])
    lifespan_df['lifespan_days'] = (lifespan_df['max'] - lifespan_df['min']).dt.days
    repeat_customers_lifespan = lifespan_df[lifespan_df['lifespan_days'] > 0]
    avg_lifespan = repeat_customers_lifespan['lifespan_days'].mean() if len(repeat_customers_lifespan) > 0 else 0

    overall_min = df['Date'].min()
    overall_max = df['Date'].max()

    print("=== Cache Analysis ===")
    print(f"Records in cache: {len(df)}")
    print(f"Date range in cache: {overall_min} -> {overall_max}")
    print(f"Unique customers: {unique_customers}")
    print(f"Repeat customers (visit-day based): {repeat_customers} ({repeat_pct:.2f}%)")
    print(f"Repeat customers (transaction based): {repeat_trx_count} ({repeat_pct_trx:.2f}%)")
    print(f"Avg lifespan (days, repeat customers only): {avg_lifespan:.2f}")

if __name__ == '__main__':
    main()
