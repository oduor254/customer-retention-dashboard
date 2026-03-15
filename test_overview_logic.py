
import data
import pandas as pd
import numpy as np

# Create dummy data
dates = pd.date_range(start='2025-04-01', periods=10, freq='D').tolist() * 2
data_dict = {
    'Date': dates,
    'Shop': ['Hazina'] * 10 + ['Hilton'] * 10,
    'Customer_ID': [f'C{i}' for i in range(20)],
    'Price': [1000] * 20,
    'Items': ['Product A'] * 10 + ['Product B'] * 10
}
df = pd.DataFrame(data_dict)

try:
    overview = data.calculate_monthly_shop_overview(df)
    print("Dummy Data Overview Structure:")
    print(overview.keys())
    
    if 'customers' in overview:
        print(f"Months: {overview['customers'].get('months', [])}")
        print(f"Shops: {list(overview['customers'].get('shops', {}).keys())}")
        
    if 'products' in overview:
        print(f"Products: {list(overview['products'].get('products', {}).keys())}")
        
    # Check if YearMonth formatting is consistent
    months = overview['customers'].get('months', [])
    if months:
        m = months[0]
        shop_val = overview['customers']['shops']['Hazina'].get(m)
        print(f"Value for shop Hazina in month {m}: {shop_val}")

except Exception as e:
    import traceback
    print(f"Error: {e}")
    traceback.print_exc()
