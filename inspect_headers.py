
import gspread
from google.oauth2.service_account import Credentials
import os

JSON_FILE_PATH = "C:\\Users\\Oduor\\Downloads\\JSON Files\\retention-484110-9e4520124486.json"
SHEET_NAME = 'Customer Database'
WORKSHEET_NAME = 'Shops'

try:
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file(JSON_FILE_PATH, scopes=SCOPES)
    client = gspread.authorize(creds)
    spreadsheet = client.open(SHEET_NAME)
    worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
    
    headers = worksheet.row_values(1)
    print(f"Headers: {headers}")
    
    first_row = worksheet.row_values(2)
    print(f"First data row: {first_row}")

except Exception as e:
    print(f"Error: {e}")
