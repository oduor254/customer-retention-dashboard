from google.oauth2.service_account import Credentials
import gspread
import pandas as pd

JSON_FILE = r'C:\Users\Administrator\Downloads\retention-485013-974e48474123.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

creds = Credentials.from_service_account_file(JSON_FILE, scopes=SCOPES)
client = gspread.authorize(creds)

# Open the correct sheet
try:
    spreadsheet = client.open('Customer Database')
    worksheet = spreadsheet.worksheet('Shops')
    
    # Get all data
    data = worksheet.get_all_records()
    df = pd.DataFrame(data)
    
    print(f"Total rows: {len(df)}")
    print(f"\nColumns in the sheet:")
    for col in df.columns:
        print(f"  - {col}")
    
    print(f"\nFirst few rows:")
    print(df.head())
    
    print(f"\nData types:")
    print(df.dtypes)
    
    print(f"\nSample 'Price' values (if exists):")
    if 'Price' in df.columns:
        print(df['Price'].head(10))
    else:
        print("'Price' column not found!")

    if 'Location' in df.columns:
        print(f"\nUnique Location values: {df['Location'].unique()}")
    else:
        print("'Location' column not found!")

    if 'Shop' in df.columns:
        print(f"\nUnique Shop values: {df['Shop'].unique()}")
    else:
        print("'Shop' column not found!")

    if 'Date' in df.columns:
        print(f"\nDate range: {df['Date'].min()} to {df['Date'].max()}")
    else:
        print("'Date' column not found!")
        
except Exception as e:
    import traceback
    print(f'Error: {e}')
    traceback.print_exc()
