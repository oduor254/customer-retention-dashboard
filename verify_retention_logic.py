import pandas as pd
import sys
import os

# Add current directory to path so we can import app if needed, 
# but here we will just copy the function or import it if possible.
# Since app.py has global code that runs on import (Flask app init, etc), 
# it might be safer to just redefine the function here for pure logic testing 
# OR use the one from app.py if we can avoid side effects.
# app.py seems to have side effects at module level? 
# "app = Flask(__name__)" is fine.
# But "creds = ..." might fail if file not found.
# Let's just copy the function logic to verify IT specifically, to avoid environment dependency issues.
# If I import app, I might trigger GSheet auth.

def calculate_cumulative_retention(df, start_date='2025-04-01'):
    """
    Calculate retention from a fixed start date (default April 1, 2025).
    Tracks 'returning customers' relative to the start date on a monthly basis.
    Includes breakdown of Revenue, New vs Returning customers.
    """
    try:
        df_copy = df.copy()
        # Ensure dates are datetime
        df_copy['Date'] = pd.to_datetime(df_copy['Date'])
        start_date_dt = pd.to_datetime(start_date)
        
        # Filter data to start from start_date
        df_filtered = df_copy[df_copy['Date'] >= start_date_dt].copy()
        
        if df_filtered.empty:
            return []
            
        df_filtered['YearMonth'] = df_filtered['Date'].dt.to_period('M')
        months = sorted(df_filtered['YearMonth'].unique())
        
        cumulative_results = []
        seen_customer_ids = set() # Customers seen since start_date
        
        for i, month in enumerate(months):
            month_df = df_filtered[df_filtered['YearMonth'] == month]
            
            # Total stats for the month
            total_customers = len(month_df['Customer_ID'].unique())
            total_revenue = month_df['Price'].sum()
            
            # Identify customers in this month
            current_customers = set(month_df['Customer_ID'].unique())
            
            if i == 0:
                # First month: All are "new" to this analysis window
                retained_customers = set()
                retained_count = 0
                retention_pct = 0
                
                new_customers = current_customers
                new_count = len(new_customers)
                new_pct = 100.0
            else:
                # Identify retained (seen before) vs new (not seen before in this window)
                retained_customers = current_customers.intersection(seen_customer_ids)
                retained_count = len(retained_customers)
                retention_pct = (retained_count / total_customers * 100) if total_customers > 0 else 0
                
                new_customers = current_customers - seen_customer_ids
                new_count = len(new_customers)
                new_pct = (new_count / total_customers * 100) if total_customers > 0 else 0
            
            # Revenue Breakdown
            # Filter rows where Customer_ID is in retained set
            retained_revenue = month_df[month_df['Customer_ID'].isin(retained_customers)]['Price'].sum()
            retained_revenue_pct = (retained_revenue / total_revenue * 100) if total_revenue > 0 else 0
            
            new_revenue = month_df[month_df['Customer_ID'].isin(new_customers)]['Price'].sum()
            new_revenue_pct = (new_revenue / total_revenue * 100) if total_revenue > 0 else 0
            
            # Update seen set
            seen_customer_ids.update(current_customers)
            
            cumulative_results.append({
                'period': str(month),
                'totalCustomers': int(total_customers),
                'retainedCustomers': int(retained_count),
                'retentionPct': round(float(retention_pct), 2),
                'newCustomers': int(new_count),
                'newPct': round(float(new_pct), 2),
                'totalRevenue': round(float(total_revenue), 2),
                'retainedRevenue': round(float(retained_revenue), 2),
                'retainedRevenuePct': round(float(retained_revenue_pct), 2),
                'newRevenue': round(float(new_revenue), 2),
                'newRevenuePct': round(float(new_revenue_pct), 2)
            })
            
        return cumulative_results

    except Exception as e:
        print(f"[ERROR] Error in calculate_cumulative_retention: {str(e)}")
        return []

def run_test():
    print("Running verification test for cumulative retention...")
    
    # Mock Data
    # Customer A: April, May, June
    # Customer B: May, June
    # Customer C: June
    # Customer D: April (Churned)
    
    data = {
        'Date': [
            '2025-04-10', '2025-04-15', # A, D
            '2025-05-10', '2025-05-12', # A, B
            '2025-06-01', '2025-06-05', '2025-06-10' # A, B, C
        ],
        'Customer_ID': [
            'CustA', 'CustD',
            'CustA', 'CustB',
            'CustA', 'CustB', 'CustC'
        ],
        'Price': [100] * 7
    }
    
    df = pd.DataFrame(data)
    
    print("Mock Data:")
    print(df)
    
    # Run Calculation
    results = calculate_cumulative_retention(df, start_date='2025-04-01')
    
    print("\nResults:")
    for res in results:
        print(res)
        
    # Validation
    # April: A, D. Total 2. Retained 0. Seen {A, D}. Rev 200.
    # May: A, B. Total 2. Retained 1 (A). Retention 50%. Seen {A, D, B}. Rev 200. Retained Rev 100.
    # June: A, B, C. Total 3. Retained 2 (A, B). Retention 66.67%. Seen {A, D, B, C}. Rev 300. Retained Rev 200.
    
    expected_retention_pct = [0, 50.0, 66.67] # Apr, May, Jun
    expected_retained_rev = [0, 100, 200]
    
    passed = True
    for i, res in enumerate(results):
        print(f"Month {i+1}: Ret%={res['retentionPct']}, RetRev={res['retainedRevenue']}")
        
        if res['retentionPct'] != expected_retention_pct[i]:
            print(f"Mismatch in Retention Pct Month {i+1}: Expected {expected_retention_pct[i]}, Got {res['retentionPct']}")
            passed = False
            
        if res['retainedRevenue'] != expected_retained_rev[i]:
             print(f"Mismatch in Retained Rev Month {i+1}: Expected {expected_retained_rev[i]}, Got {res['retainedRevenue']}")
             passed = False
            
    if passed:
        print("\nTEST PASSED! Logic works as expected.")
    else:
        print("\nTEST FAILED!")

if __name__ == "__main__":
    run_test()
