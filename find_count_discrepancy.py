import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import certifi

# Configuration
JSON_FILE_PATH = r'C:\Users\Oduor\Downloads\JSON Files\retention-484110-9e4520124486.json'
SHEET_NAME = 'Customer Database'
WORKSHEET_NAME = 'Shops'
# Optional: Configure the start year for analysis. If not set, include all history.
DATA_START_YEAR_ENV = os.environ.get("DATA_START_YEAR")
try:
    DATA_START_YEAR = int(DATA_START_YEAR_ENV) if DATA_START_YEAR_ENV else None
except ValueError:
    DATA_START_YEAR = None

def find_discrepancy():
    # SSL fixes
    for _bad_ev in ('CURL_CA_BUNDLE', 'REQUESTS_CA_BUNDLE'):
            os.environ.pop(_bad_ev, None)
    os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()
    os.environ['SSL_CERT_FILE']      = certifi.where()

    SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 
              'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file(JSON_FILE_PATH, scopes=SCOPES)
    client = gspread.authorize(creds)
    
    spreadsheet = client.open(SHEET_NAME)
    worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
    data = worksheet.get_all_records()
    df = pd.DataFrame(data)
    
    if 'Location' in df.columns:
        df.rename(columns={'Location': 'Shop'}, inplace=True)
    
    rejects_raw = df[df['Shop'].astype(str).str.contains('Rejects', case=False, na=False)]
    rejects_raw['Date'] = pd.to_datetime(rejects_raw['Date'], errors='coerce')
    
    print(f"Total raw Rejects records: {len(rejects_raw)}")
    print(f"Total unique raw Phones in Rejects: {rejects_raw['Phone'].astype(str).str.strip().nunique()}")
    
    # Filter out organizations and optionally apply a start year cutoff
    filters = rejects_raw['Female'].astype(str).str.lower().str.strip() != 'organization'
    if DATA_START_YEAR is not None:
        filters = filters & (rejects_raw['Date'].dt.year >= DATA_START_YEAR)
    df_filtered = rejects_raw[filters].copy()
    
    print(f"Total filtered Rejects records: {len(df_filtered)}")
    print(f"Total unique filtered Phones: {df_filtered['Phone'].astype(str).str.strip().nunique()}")
    
    # Find the excluded ones
    excluded = rejects_raw[~rejects_raw.index.isin(df_filtered.index)]
    if not excluded.empty:
        print("\nExcluded records that caused the discrepancy:")
        print(excluded[['Date', 'First Name', 'Phone', 'Female']])
    else:
        print("\nNo records excluded by filters.")

if __name__ == "__main__":
    find_discrepancy()
