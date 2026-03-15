from google.oauth2.service_account import Credentials
import gspread

JSON_FILE = r'C:\Users\Oduor\Downloads\JSON Files\retention-484110-9e4520124486.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

try:
    creds = Credentials.from_service_account_file(JSON_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)
    
    # List all accessible spreadsheets
    spreadsheets = client.openall()
    print('Available Spreadsheets:')
    for sheet in spreadsheets:
        print(f"  - {sheet.title}")
        for worksheet in sheet.worksheets():
            print(f"    - {worksheet.title}")
except Exception as e:
    print(f'Error: {e}')
