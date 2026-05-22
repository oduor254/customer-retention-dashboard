import pandas as pd
import numpy as np

df = pd.read_csv('customer_data_cache.csv', low_memory=False)
df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
df = df[df['Price'].notna()]

lifespan_df = df.groupby('Customer_ID')['Date'].agg(['min', 'max'])
lifespan_df['lifespan_days'] = (lifespan_df['max'] - lifespan_df['min']).dt.days

# --- Try different minimum lifespan thresholds ---
print("=== Mean lifespan by min threshold ===")
for threshold in [0, 1, 7, 14, 30, 60, 90]:
    subset = lifespan_df[lifespan_df['lifespan_days'] > threshold]
    print(f"  lifespan > {threshold:3d}d : mean={subset['lifespan_days'].mean():.2f}  n={len(subset)}")

# --- Try filtering by year ---
print("\n=== By year (repeat customers >0) ===")
for year in df['Date'].dt.year.dropna().unique():
    year = int(year)
    df_yr = df[df['Date'].dt.year == year]
    ls = df_yr.groupby('Customer_ID')['Date'].agg(['min', 'max'])
    ls['lifespan_days'] = (ls['max'] - ls['min']).dt.days
    repeat = ls[ls['lifespan_days'] > 0]
    print(f"  {year}: mean={repeat['lifespan_days'].mean():.2f}  n={len(repeat)}")

# --- Try visit-count based: customers with >= 2 transactions (not visit days) ---
print("\n=== By transaction count filter ===")
trx_counts = df.groupby('Customer_ID').size()
for min_trx in [2, 3, 4, 5]:
    cids = trx_counts[trx_counts >= min_trx].index
    subset = lifespan_df[lifespan_df.index.isin(cids)]
    repeat = subset[subset['lifespan_days'] > 0]
    print(f"  >= {min_trx} transactions: mean={repeat['lifespan_days'].mean():.2f}  n={len(repeat)}")

# --- Try recent data only (2025+) ---
print("\n=== 2025+ data only (repeat >0) ===")
df_2025 = df[df['Date'].dt.year >= 2025]
ls2025 = df_2025.groupby('Customer_ID')['Date'].agg(['min', 'max'])
ls2025['lifespan_days'] = (ls2025['max'] - ls2025['min']).dt.days
repeat_2025 = ls2025[ls2025['lifespan_days'] > 0]
print(f"  2025+ repeat-only: mean={repeat_2025['lifespan_days'].mean():.2f}  n={len(repeat_2025)}")

# --- Use FULL date range for customers seen in 2025+ ---
print("\n=== Customers active in 2025+, full lifespan (all history) ===")
active_2025_ids = df_2025['Customer_ID'].unique()
ls_full = lifespan_df[lifespan_df.index.isin(active_2025_ids)]
repeat_full = ls_full[ls_full['lifespan_days'] > 0]
print(f"  mean={repeat_full['lifespan_days'].mean():.2f}  n={len(repeat_full)}")
