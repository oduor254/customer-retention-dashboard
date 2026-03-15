
import data
import pandas as pd

try:
    df = data.get_customer_data()
    print(f"Dataframe loaded with {len(df)} records")
    print(f"Columns: {df.columns.tolist()}")
    print("\nUnique Shops in Data:")
    print(df['Shop'].unique())
    print("\nShops in SHOP_REGION_MAP:")
    print(list(data.SHOP_REGION_MAP.keys()))
    
    print("\nSample Data (First 3 rows):")
    print(df.head(3))
    
    print("\nDate Range:")
    print(f"Min: {df['Date'].min()}")
    print(f"Max: {df['Date'].max()}")

    # Check monthly overview logic
    df_copy = df.copy()
    df_copy['YearMonth'] = df_copy['Date'].dt.to_period('M')
    shops = sorted([s for s in df_copy['Shop'].unique() if s in data.SHOP_REGION_MAP])
    print(f"\nShops matching MAP: {shops}")
    months = sorted(df_copy['YearMonth'].unique())
    print(f"Months found: {months}")

except Exception as e:
    import traceback
    print(f"Error: {e}")
    traceback.print_exc()
