import pandas as pd
import numpy as np
import requests
import json
from datetime import datetime

# Fetch data using the API to ensure consistency with the app
try:
    print("Fetching data from API...")
    # We'll use the get_customer_data logic directly to get the DataFrame for analysis
    # since we can't easily get the raw DF from the API endpoint which returns JSON.
    # So we will replicate the loading logic mainly or simple just use the verify logic but enhanced.
    
    # Actually, let's just use the app's logic by importing it, if possible.
    # But environment setup might be tricky (flask app context).
    # Let's write a script that does the independent analysis on the Google Sheet data if possible, 
    # OR simpler: modify app.py temporarily to print debug stats? 
    # No, better to write a standalone script that imports get_customer_data from app.py
    
    from app import get_customer_data
    
    print("Loading data...")
    df = get_customer_data()
    
    # Ensure Price cleaning is consistent (though get_customer_data now has it)
    
    print(f"Total records: {len(df)}")
    
    df['Visit_Date'] = df['Date'].dt.date
    
    # Analyze Purchase Cycle
    visit_counts = df.groupby('Customer_ID')['Visit_Date'].nunique()
    lifespan_df = df.groupby('Customer_ID')['Date'].agg(['min', 'max'])
    lifespan_df['lifespan_days'] = (lifespan_df['max'] - lifespan_df['min']).dt.days
    
    customer_metrics = pd.concat([lifespan_df, visit_counts], axis=1)
    customer_metrics.rename(columns={'Visit_Date': 'visit_count'}, inplace=True)
    
    # Filter for repeats
    repeat_metrics = customer_metrics[customer_metrics['visit_count'] > 1].copy()
    
    print(f"Total Customers: {len(customer_metrics)}")
    print(f"Repeat Customers (>1 visit): {len(repeat_metrics)}")
    
    if len(repeat_metrics) > 0:
        repeat_metrics['purchase_cycle'] = repeat_metrics['lifespan_days'] / (repeat_metrics['visit_count'] - 1)
        
        cycle_mean = repeat_metrics['purchase_cycle'].mean()
        cycle_median = repeat_metrics['purchase_cycle'].median()
        cycle_min = repeat_metrics['purchase_cycle'].min()
        cycle_max = repeat_metrics['purchase_cycle'].max()
        cycle_std = repeat_metrics['purchase_cycle'].std()
        
        print(f"\n--- Purchase Cycle Statistics ---")
        print(f"Mean: {cycle_mean:.2f} days")
        print(f"Median: {cycle_median:.2f} days")
        print(f"Min: {cycle_min:.2f} days")
        print(f"Max: {cycle_max:.2f} days")
        print(f"Std Dev: {cycle_std:.2f} days")
        
        print(f"\n--- Percentiles ---")
        print(repeat_metrics['purchase_cycle'].quantile([0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]))
        
        print(f"\n--- Top 10 Largest Cycles ---")
        print(repeat_metrics.nlargest(10, 'purchase_cycle')[['lifespan_days', 'visit_count', 'purchase_cycle']])
        
        # Check Date Distribution
        print(f"\n--- Date Range ---")
        print(f"Min Date: {df['Date'].min()}")
        print(f"Max Date: {df['Date'].max()}")
        
    else:
        print("No repeat customers found.")

except Exception as e:
    print(f"Error: {e}")
