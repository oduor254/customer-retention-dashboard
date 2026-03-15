from google.oauth2.service_account import Credentials
import gspread

JSON_FILE = r'C:\Users\Administrator\Downloads\sales-484512-a64c57dd56fc.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

creds = Credentials.from_service_account_file(JSON_FILE, scopes=SCOPES)
client = gspread.authorize(creds)

# List all accessible spreadsheets
try:
    spreadsheets = client.list_spreadsheet_files()
    print('Available Spreadsheets:')
    for sheet in spreadsheets[:10]:
        print(f"  - {sheet['name']}")
except Exception as e:
    print(f'Error: {e}')
