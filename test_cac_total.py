from data import get_customer_data, calculate_overview
import pandas as pd

try:
    df = get_customer_data()
    overview = calculate_overview(df)
    print("--- Overview Stats ---")
    print(f"Total Unique Customers: {overview['uniqueCustomers']}")
    print(f"Total Marketing Spend: {overview['marketingSpend']}")
    print(f"Total CAC: {overview['cac']}")
    
    if 'MARKETING EXPENSE' in df.columns:
        print("\n--- Marketing Data Head ---")
        print(df[['Date', 'MARKETING EXPENSE']].dropna().drop_duplicates('Date').head())
    else:
        print("\n'MARKETING EXPENSE' column not found in DataFrame!")
        print(f"Available columns: {df.columns.tolist()}")

except Exception as e:
    print(f"Error: {e}")
