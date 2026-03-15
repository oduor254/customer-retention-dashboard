import pandas as pd
from data import get_customer_data

def inspect_rejects_counts():
    df = get_customer_data()
    rejects_df = df[df['Shop'] == 'Rejects']
    
    unique_ids = rejects_df['Customer_ID'].unique()
    total_unique = len(unique_ids)
    
    print(f"Total records for Rejects: {len(rejects_df)}")
    print(f"Unique Customer_IDs for Rejects: {total_unique}")
    
    # Check for empty or invalid phone numbers
    invalid_phones = rejects_df[rejects_df['Phone'].astype(str).str.len() < 5]
    if not invalid_phones.empty:
        print(f"\nInvalid/Short Phone numbers ({len(invalid_phones)}):")
        print(invalid_phones[['Date', 'First Name', 'Phone', 'Product']])
        
    # Check for organization (already filtered in get_customer_data but let's verify)
    orgs = rejects_df[rejects_df['Gender'].str.lower().str.strip() == 'organization']
    print(f"\nOrganization records: {len(orgs)}")

    # Check for duplicates in the unique list
    print("\nSample of unique IDs:")
    print(unique_ids[:10])
    
    # Let's see if there are records that might be causing the +2 difference
    # Maybe some Phone numbers are just "nan" or "None"
    missing_phone = rejects_df[rejects_df['Phone'].isna()]
    print(f"\nRecords with missing Phone: {len(missing_phone)}")

if __name__ == "__main__":
    inspect_rejects_counts()
