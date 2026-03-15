import pandas as pd
from data import get_customer_data, calculate_shop_loyalty_analysis, calculate_overview

def verify_counts():
    print("Verifying 'Rejects' customer counts...")
    df = get_customer_data()
    
    # Filter for Rejects
    rejects_df = df[df['Shop'] == 'Rejects']
    
    # 1. Manual check of unique Customer_ID
    unique_ids = rejects_df['Customer_ID'].unique()
    print(f"Manual count of unique Customer_IDs in Rejects: {len(unique_ids)}")
    
    # Check for any duplicates in phone numbers that might not be stripped correctly
    raw_phones = rejects_df['Phone'].astype(str).unique()
    print(f"Manual count of unique raw Phone strings: {len(raw_phones)}")
    
    # 2. Check calculate_overview output
    overview = calculate_overview(rejects_df)
    print(f"calculate_overview(rejects_df) uniqueCustomers: {overview['uniqueCustomers']}")
    
    # 3. Check calculate_shop_loyalty_analysis output
    loyalty = calculate_shop_loyalty_analysis(df, 'Rejects')
    print(f"calculate_shop_loyalty_analysis(df, 'Rejects') totalCustomers: {loyalty['totalCustomers']}")
    
    if len(unique_ids) != 401:
        print("\nDiscrepancy found! Listing details to identify the 2 extra customers...")
        # Let's see if there are any suspicious values
        print("\nPhone numbers sorted:")
        print(sorted(unique_ids)[:10])
        print("...")
        print(sorted(unique_ids)[-10:])
        
        # Check for non-numeric characters in phone
        non_numeric = rejects_df[~rejects_df['Phone'].astype(str).str.isnumeric()]
        if not non_numeric.empty:
            print(f"\nNon-numeric Phone numbers found: {len(non_numeric)}")
            print(non_numeric[['First Name', 'Phone', 'Product']])

    # Check if there are duplicate Phone numbers with different names
    # (The code uses Phone as Customer_ID, so this shouldn't increase the count, 
    # but let's check if there's any other ID logic)
    
if __name__ == "__main__":
    verify_counts()
