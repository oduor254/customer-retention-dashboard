
import data
from flask import Flask
import json

app = Flask(__name__)

with app.app_context():
    # Simulate API call
    df = data.get_customer_data()
    overview = data.calculate_monthly_shop_overview(df)
    
    print("Monthly Overview Structure (Keys):")
    print(overview.keys())
    
    if 'customers' in overview:
        print(f"Months in overview: {overview['customers'].get('months', [])}")
        print(f"Number of shops: {len(overview['customers'].get('shops', {}))}")
    
    if 'products' in overview:
        print(f"Number of products: {len(overview['products'].get('products', {}))}")

    # Save to a file for investigation
    with open('api_response_sample.json', 'w') as f:
        json.dump(overview, f, indent=4)
    print("\nFull overview saved to api_response_sample.json")
