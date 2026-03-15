
import data
import pandas as pd

try:
    df = data.get_customer_data()
    print("Columns found in Google Sheet:")
    print(df.columns.tolist())
    print("\nSample Data:")
    print(df.head(5))
    
    # Check for date range
    print("\nDate Range:")
    print(f"Min Date: {df['Date'].min()}")
    print(f"Max Date: {df['Date'].max()}")
    
    # Check for product column candidates
    possible_columns = ['Product', 'Item', 'Product Name', 'Item Name', 'ProductName', 'ItemName', 'Items']
    found = [c for c in possible_columns if c in df.columns]
    print(f"\nPotential Product Columns found: {found}")
    
    # Check if we have data after April 2025
    oct_2025 = pd.to_datetime('2025-04-01')
    later_df = df[df['Date'] >= oct_2025]
    print(f"\nRecords after April 2025: {len(later_df)}")

except Exception as e:
    print(f"Error: {e}")
