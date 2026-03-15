import requests
import json

try:
    print("Fetching /api/data...")
    response = requests.get('http://localhost:5002/api/data')
    data = response.json()
    
    overview = data.get('overview', {})
    print("\n--- JSON Result for /api/data ---")
    print(f"Overview Length Keys: {len(overview)}")
    print(f"marketingSpend: {overview.get('marketingSpend')}")
    print(f"cac: {overview.get('cac')}")
    
except Exception as e:
    print(f"Error: {e}")
