"""
Diagnose Total Marketing Spend discrepancy.
Dashboard: 32,466,879.8
Sheets:    32,541,775.80
Diff:         ~74,896.00
"""
import pandas as pd

CACHE_FILE = 'customer_data_cache.csv'
TARGET_SHEETS = 32_541_775.80

df = pd.read_csv(CACHE_FILE)
df['Date'] = pd.to_datetime(df['Date'], errors='coerce')

if 'MARKETING EXPENSE' not in df.columns:
    print("ERROR: 'MARKETING EXPENSE' column not found!")
    exit()

# Clean the column the same way data.py does
df['MARKETING EXPENSE'] = (
    df['MARKETING EXPENSE']
    .astype(str)
    .str.replace(r'[^\d.]', '', regex=True)
)
df['MARKETING EXPENSE'] = pd.to_numeric(df['MARKETING EXPENSE'], errors='coerce').fillna(0)

# --- Current (buggy) calculation ---
current_calc = df.groupby(df['Date'].dt.date)['MARKETING EXPENSE'].max().sum()
print(f"\n=== CURRENT (max per day)         : {current_calc:,.2f}")
print(f"    Sheets target                  : {TARGET_SHEETS:,.2f}")
print(f"    Difference                     : {TARGET_SHEETS - current_calc:,.2f}")

# --- Option A: simple sum of all rows ---
simple_sum = df['MARKETING EXPENSE'].sum()
print(f"\n=== OPTION A: simple row sum       : {simple_sum:,.2f}")
print(f"    Difference from sheets         : {TARGET_SHEETS - simple_sum:,.2f}")

# --- Option B: max per (Date, Shop) then sum ---
if 'Shop' in df.columns:
    per_shop_day = df.groupby([df['Date'].dt.date, 'Shop'])['MARKETING EXPENSE'].max().sum()
    print(f"\n=== OPTION B: max per (Date,Shop)  : {per_shop_day:,.2f}")
    print(f"    Difference from sheets         : {TARGET_SHEETS - per_shop_day:,.2f}")

# --- Option C: sum of unique (Date, Amount) pairs ---
dedup = df.drop_duplicates(subset=[df['Date'].dt.date.rename('day'), 'MARKETING EXPENSE'])
# rebuild with date-as-date column
df['_day'] = df['Date'].dt.date
dedup2 = df.drop_duplicates(subset=['_day', 'MARKETING EXPENSE'])
dedup_sum = dedup2['MARKETING EXPENSE'].sum()
print(f"\n=== OPTION C: unique (date,amount) : {dedup_sum:,.2f}")
print(f"    Difference from sheets         : {TARGET_SHEETS - dedup_sum:,.2f}")

# --- Show days where max != sum ---
day_max  = df.groupby('_day')['MARKETING EXPENSE'].max()
day_sum  = df.groupby('_day')['MARKETING EXPENSE'].sum()
day_diff = day_sum - day_max
problem_days = day_diff[day_diff > 0].sort_values(ascending=False)

print(f"\n=== Days where sum > max (missed spend): {len(problem_days)}")
print(problem_days.head(20).to_string())

# --- For the biggest problem days, show shop breakdown ---
if len(problem_days) > 0 and 'Shop' in df.columns:
    print("\n=== Shop breakdown on top problem days ===")
    for day in problem_days.head(5).index:
        sub = df[df['_day'] == day][['Shop', 'MARKETING EXPENSE']].drop_duplicates()
        print(f"\n{day}  (missed: {problem_days[day]:,.2f})")
        print(sub.to_string(index=False))
