from flask import Flask, render_template, jsonify, request
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import numpy as np
from datetime import datetime
import json
import time
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import requests
import io
import os
import threading

app = Flask(__name__)

# Configuration
JSON_FILE_PATH = r'C:\Users\Oduor\Downloads\JSON Files\retention-484110-9e4520124486.json'
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
SHEET_NAME = 'Customer Database'
WORKSHEET_NAME = 'Shops'

# Currency conversion rate (USD to KES)
USD_TO_KES = 1/130

# Color mapping by region
REGION_COLORS = {
    'Nairobi CBD': '#3498db',           # Blue
    'Coastal Region': '#e74c3c',        # Red
    'Western & Nyanza': '#2ecc71',      # Green
    'Central Region': '#f39c12',        # Orange
    'Rift Valley': '#9b59b6',           # Purple
    'Diaspora': '#1abc9c', 
    'Website': '#27C2F5', 
    'Rejects': '#27C2F5',             # Turquoise
}

# Shop to Region mapping
SHOP_REGION_MAP = {
    'Hazina': 'Nairobi CBD',
    'Hilton': 'Nairobi CBD',
    'Starmall': 'Nairobi CBD',
    'Ktda': 'Nairobi CBD',
    'Rejects': 'Rejects',
    'Mombasa': 'Coastal Region',
    'Kakamega': 'Western & Nyanza',
    'Kisumu': 'Western & Nyanza',
    'Kisii': 'Western & Nyanza',
    'Meru': 'Central Region',
    'Nanyuki': 'Central Region',
    'Thika': 'Central Region',
    'Eldoret': 'Rift Valley',
    'Nakuru': 'Rift Valley',
    'Kitengela': 'Rift Valley',
    'Sinza': 'Diaspora',
    'Tanzania': 'Diaspora',
    'Uganda': 'Diaspora',
    'Website': 'Online',
}

# Cache variables
cached_data = None
last_fetch_time = None
computed_results_cache = None  # Store final aggregated JSON results
CACHE_DURATION = 1800  # Cache for 30 minutes to reduce slow network calls
CACHE_FILE = 'customer_data_cache.csv'


def get_customer_data():
    """Fetch and process customer data from Google Sheets with persistent caching"""
    global cached_data, last_fetch_time
    
    # 1. Return in-memory cache if valid
    if cached_data is not None and last_fetch_time is not None:
        if time.time() - last_fetch_time < CACHE_DURATION:
            print("[DEBUG] Returning in-memory cached data")
            return cached_data
    
    # 2. Try to load from persistent cache if available and memory cache is empty
    # import os (moved to top-level)
    if cached_data is None and os.path.exists(CACHE_FILE):
        try:
            print("[INFO] Loading from persistent cache...")
            df_persistent = pd.read_csv(CACHE_FILE)
            df_persistent['Date'] = pd.to_datetime(df_persistent['Date'], errors='coerce')
            cached_data = df_persistent
            last_fetch_time = os.path.getmtime(CACHE_FILE)
            print(f"[INFO] Loaded {len(df_persistent)} records from persistent cache")
            
            # If the file is very fresh, just return it
            if time.time() - last_fetch_time < CACHE_DURATION:
                return cached_data
        except Exception as e:
            print(f"[ERROR] Failed to load persistent cache: {e}")

    try:
        # Fix: Clear any bad SSL env vars (e.g. CURL_CA_BUNDLE set by PostgreSQL install)
        # that break HTTPS calls to Google APIs, and use Python's own certifi bundle instead.
        import certifi
        for _bad_ev in ('CURL_CA_BUNDLE', 'REQUESTS_CA_BUNDLE'):
            os.environ.pop(_bad_ev, None)
        os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()
        os.environ['SSL_CERT_FILE']      = certifi.where()
        print(f"[INFO] SSL CA bundle set to: {certifi.where()}")

        print("[INFO] Fetching fresh data from Google Sheets...")
        # Setup Google Sheets authentication
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 
                  'https://www.googleapis.com/auth/drive']
        
        if os.path.exists(JSON_FILE_PATH):
            # Local development
            creds = Credentials.from_service_account_file(JSON_FILE_PATH, scopes=SCOPES)
        elif GOOGLE_SERVICE_ACCOUNT_JSON:
            # Render / cloud deployment
            creds_info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
            creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
        else:
            raise ValueError("No Google credentials found.")
        
        # Create an authorized session from google-auth
        from google.auth.transport.requests import AuthorizedSession
        import socket
        
        # Set a global socket timeout as a safety net
        socket.setdefaulttimeout(120)
        
        # Custom timeout adapter
        class TimeoutHTTPAdapter(HTTPAdapter):
            def __init__(self, timeout=None, *args, **kwargs):
                self.timeout = timeout
                super().__init__(*args, **kwargs)
            
            def send(self, request, **kwargs):
                # Ensure timeout is always applied, even if passed as None or omitted
                if kwargs.get('timeout') is None:
                    kwargs['timeout'] = self.timeout
                return super().send(request, **kwargs)
        
        # Increase retries and backoff significantly to handle unstable connections
        retry_strategy = Retry(
            total=5,  
            backoff_factor=2,  # Wait 2s, 4s, 8s, 16s...
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
            raise_on_status=False
        )
        
        # Create the session with timeout adapter
        authorized_session = AuthorizedSession(creds)
        
        # Use custom adapter with 90 second timeout
        timeout_adapter = TimeoutHTTPAdapter(timeout=90, max_retries=retry_strategy)
        authorized_session.mount("https://", timeout_adapter)
        authorized_session.mount("http://", timeout_adapter)
        
        # Initialize gspread with the authorized session
        client = gspread.Client(auth=creds)
        client.session = authorized_session
        # CRITICAL: Set explicit timeout on gspread client to prevent it from passing None
        client.http_client.timeout = 90
        
        print(f"[INFO] Opening spreadsheet '{SHEET_NAME}'...")
        # Open the spreadsheet and specific worksheet
        spreadsheet = client.open(SHEET_NAME)
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
        
        # Get all data
        data = worksheet.get_all_records()
        df = pd.DataFrame(data)
        
        # Handle column naming mismatch (Sheet uses 'Location', code uses 'Shop')
        if 'Shop' not in df.columns and 'Location' in df.columns:
            print("[INFO] Renaming 'Location' column to 'Shop'")
            df.rename(columns={'Location': 'Shop'}, inplace=True)
            
        if 'Gender' not in df.columns and 'Female' in df.columns:
            print("[INFO] Renaming 'Female' column to 'Gender'")
            df.rename(columns={'Female': 'Gender'}, inplace=True)
            
        if 'Shop' in df.columns:
            # Normalize shop names to Title Case and strip whitespace
            df['Shop'] = df['Shop'].astype(str).str.strip().str.title()
            
        if df.empty:
            raise ValueError("No data returned from Google Sheets")
        
        print(f"[INFO] Successfully loaded {len(df)} records from Google Sheets")
        
        # Convert Date column to datetime
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        
        # Filter out organizations and only 2025-2026 data
        df_filtered = df[
            (df['Gender'].str.lower().str.strip() != 'organization') & 
            (df['Date'].dt.year >= 2025)
        ].copy()
        
        if df_filtered.empty:
            raise ValueError("No data matches the filter criteria after processing")
        
        # Process prices
        df_filtered['Price'] = df_filtered['Price'].astype(str).str.replace(r'[^\d.]', '', regex=True)
        df_filtered['Price'] = pd.to_numeric(df_filtered['Price'], errors='coerce')
        
        # Process Marketing Expense if present
        if 'MARKETING EXPENSE' in df_filtered.columns:
            df_filtered['MARKETING EXPENSE'] = df_filtered['MARKETING EXPENSE'].astype(str).str.replace(r'[^\d.]', '', regex=True)
            df_filtered['MARKETING EXPENSE'] = pd.to_numeric(df_filtered['MARKETING EXPENSE'], errors='coerce').fillna(0)
            
        # Create customer identifier - Using only Phone number as requested (avoiding name duplicates)
        df_filtered['Customer_ID'] = df_filtered['Phone'].astype(str).str.strip()
        
        # 3. Update memory cache and persistent cache
        cached_data = df_filtered
        last_fetch_time = time.time()
        
        try:
            df_filtered.to_csv(CACHE_FILE, index=False)
            print("[INFO] Persistent cache updated")
        except Exception as e:
            print(f"[WARNING] Could not save to persistent cache: {e}")
            
        return df_filtered
    
    except Exception as e:
        print(f"[ERROR] Failed to fetch from Google Sheets: {str(e)}")
        
        # 4. Final Fallback: Use whatever we have in memory or on disk
        if cached_data is not None:
            print("[DEBUG] Falling back to available cached data due to error")
            return cached_data
            
        raise Exception(f"Unable to load data (Connection error and no cache available): {str(e)}")


def calculate_overview(df):
    """Calculate overview metrics"""
    
    try:
        # Make a copy to avoid modifying original
        df_work = df.copy()
        
        # Remove rows with NaN prices
        df_work = df_work[df_work['Price'].notna()]
        
        # Total transactions
        total_transactions = len(df_work)
        
        # Unique customers
        unique_customers = df_work['Customer_ID'].nunique()
        
        # Average spend per visit
        df_work['Visit_Date'] = df_work['Date'].dt.date
        visit_spending = df_work.groupby(['Customer_ID', 'Visit_Date'])['Price'].sum().reset_index()
        avg_spend_per_visit = visit_spending['Price'].mean() if len(visit_spending) > 0 else 0
        total_visits = len(visit_spending)
        
        # Average spend per customer
        customer_spending = df_work.groupby('Customer_ID')['Price'].sum()
        avg_spend_per_customer = customer_spending.mean() if len(customer_spending) > 0 else 0
        
        # Customer purchase frequency - count unique visit days per customer
        customer_visit_days = df_work.groupby('Customer_ID')['Visit_Date'].nunique()
        
        # One-timers and repeat customers (based on different days visited)
        one_timers = (customer_visit_days == 1).sum()
        repeat_customers = (customer_visit_days > 1).sum()
        one_timer_pct = (one_timers / unique_customers * 100) if unique_customers > 0 else 0
        repeat_pct = (repeat_customers / unique_customers * 100) if unique_customers > 0 else 0
        
        # Average lifespan (for repeat customers only)
        # Difference between first and last purchase date
        lifespan_df = df_work.groupby('Customer_ID')['Date'].agg(['min', 'max'])
        lifespan_df['lifespan_days'] = (lifespan_df['max'] - lifespan_df['min']).dt.days
        # Only consider customers with span > 0 (repeat customers basically, or same day repeats)
        # Better to filter for meaningful lifespan (more than 1 visit day)
        repeat_customers_lifespan = lifespan_df[lifespan_df['lifespan_days'] > 0]
        avg_lifespan = repeat_customers_lifespan['lifespan_days'].mean() if len(repeat_customers_lifespan) > 0 else 0
        
        # Average purchase cycle (for repeat customers)
        # Average time between purchases = Total Lifespan / (Number of Visits - 1)
        # We need visit counts per customer
        # Prepare metrics for cycle calculation
        customer_metrics = lifespan_df.copy()
        customer_metrics['visit_count'] = customer_visit_days
        
            # Optimized Average Purchase Cycle Calculation (Vectorized)
        # 1. Sort by Customer and Date to ensure correct diff calculation
        df_sorted = df_work.sort_values(['Customer_ID', 'Date'])
        
        # 2. Calculate time difference between consecutive rows
        df_sorted['date_diff'] = df_sorted['Date'].diff().dt.days
        
        # 3. Create a mask to identify the start of a new customer block
        # The first record for each customer should not be compared to the previous customer's last record
        mask = df_sorted['Customer_ID'] != df_sorted['Customer_ID'].shift(1)
        
        # 4. Set diff to NaN for the first visit of each customer
        df_sorted.loc[mask, 'date_diff'] = np.nan
        
        # 5. Filter for valid differences (repeat visits)
        valid_diffs = df_sorted.dropna(subset=['date_diff'])
        
        if not valid_diffs.empty:
            # 6. Calculate average cycle per customer first (to match user logic: Mean of (Mean per Customer))
            per_customer_avg = valid_diffs.groupby('Customer_ID')['date_diff'].mean()
            avg_cycle = per_customer_avg.mean()
        else:
            avg_cycle = 0
        
        total_revenue = df_work['Price'].sum()

        # TWO APPROACHES REPEAT ANALYSIS
        # 1. Transaction-Based: Any customer with > 1 total transaction
        customer_trx_counts = df_work.groupby('Customer_ID').size()
        repeat_customers_trx = (customer_trx_counts > 1).sum()
        repeat_pct_trx = (repeat_customers_trx / unique_customers * 100) if unique_customers > 0 else 0

        # 2. Visit-Day Based: Any customer with > 1 unique visit day (Current logic)
        # Already calculated above as repeat_customers and repeat_pct

        # Date range
        start_date = df_work['Date'].min().strftime('%Y-%m-%d') if not df_work.empty else "N/A"
        end_date = df_work['Date'].max().strftime('%Y-%m-%d') if not df_work.empty else "N/A"
        
        # Calculate Marketing Spend and CAC
        marketing_cost = 0
        cac = 0
        if 'MARKETING EXPENSE' in df_work.columns:
            # Aggregate unique daily spends
            marketing_cost = df_work.groupby(df_work['Date'].dt.date)['MARKETING EXPENSE'].max().sum()
            cac = marketing_cost / unique_customers if unique_customers > 0 else 0

        return {
            'totalPurchases': int(total_transactions),
            'totalVisits': int(total_visits),
            'uniqueCustomers': int(unique_customers),
            'avgSpendPerVisit': round(float(avg_spend_per_visit), 2) if not np.isnan(avg_spend_per_visit) else 0,
            'avgSpendPerCustomer': round(float(avg_spend_per_customer), 2) if not np.isnan(avg_spend_per_customer) else 0,
            'totalRevenue': round(float(total_revenue), 2),
            'oneTimers': int(one_timers),
            'oneTimerPct': round(float(one_timer_pct), 2),
            'repeatCustomers': int(repeat_customers),
            'repeatPct': round(float(repeat_pct), 2),
            'repeatCustomersTrx': int(repeat_customers_trx),
            'repeatPctTrx': round(float(repeat_pct_trx), 2),
            'avgLifespan': round(float(avg_lifespan), 2) if not np.isnan(avg_lifespan) else 0,
            'avgCycle': float(avg_cycle) if not np.isnan(avg_cycle) else 0,
            'startDate': start_date,
            'endDate': end_date,
            'marketingSpend': float(marketing_cost),
            'cac': float(cac)
        }
    except Exception as e:
        raise Exception(f"Error calculating overview: {str(e)}")

def calculate_visit_interval_distribution(df):
    """Calculate the distribution of days between visits for repeat customers"""
    try:
        df_sorted = df.sort_values(['Customer_ID', 'Date'])
        df_sorted['date_diff'] = df_sorted['Date'].diff().dt.days
        mask = df_sorted['Customer_ID'] != df_sorted['Customer_ID'].shift(1)
        df_sorted.loc[mask, 'date_diff'] = np.nan
        
        valid_diffs = df_sorted.dropna(subset=['date_diff'])['date_diff']
        
        if valid_diffs.empty:
            return []
            
        # Define bins
        bins = [0, 7, 14, 30, 60, 90, float('inf')]
        labels = ['0-7 Days', '8-14 Days', '15-30 Days', '31-60 Days', '61-90 Days', '91+ Days']
        
        dist = pd.cut(valid_diffs, bins=bins, labels=labels, right=True).value_counts().sort_index()
        
        results = []
        for label, count in dist.items():
            results.append({
                'bin': str(label),
                'count': int(count),
                'percentage': round(float(count / len(valid_diffs) * 100), 2)
            })
            
        return results
    except Exception as e:
        print(f"Error calculating interval distribution: {str(e)}")
        return []


def calculate_retention_repeat(period_df, prev_period_df=None, full_df=None):
    """Calculate retention and repeat metrics including spend, lifespan and growth"""
    
    try:
        current_customers_series = period_df['Customer_ID']
        current_customers = {cid for cid in current_customers_series.unique() if pd.notna(cid)}
        current_count = len(current_customers)
        
        # 1. Visit-Day Based Repeat
        period_df_copy = period_df.copy()
        period_df_copy['Visit_Date'] = period_df_copy['Date'].dt.date
        visit_days = period_df_copy.groupby('Customer_ID')['Visit_Date'].nunique()
        repeat_in_period = (visit_days > 1).sum()
        repeat_pct = (repeat_in_period / current_count * 100) if current_count > 0 else 0

        # 2. Transaction-Based Repeat
        trx_counts = period_df.groupby('Customer_ID').size()
        repeat_trx_count = (trx_counts > 1).sum()
        repeat_trx_pct = (repeat_trx_count / current_count * 100) if current_count > 0 else 0
        
        # 3. Avg Spend Per Customer
        total_revenue = period_df['Price'].sum()
        avg_spend = (total_revenue / current_count) if current_count > 0 else 0

        # 4. Avg Lifespan (Days) - Historical lifespan for active customers
        avg_lifespan = 0
        if full_df is not None and not current_customers_series.empty:
            active_ids = current_customers_series.unique()
            # Filter full_df for these specific customers to get their global min/max
            lifespan_data = full_df[full_df['Customer_ID'].isin(active_ids)].groupby('Customer_ID')['Date'].agg(['min', 'max'])
            lifespan_days = (lifespan_data['max'] - lifespan_data['min']).dt.days
            avg_lifespan = lifespan_days.mean() if not lifespan_days.empty else 0

        # 5. % Change (Growth Rate)
        growth_rate = 0
        if prev_period_df is not None and not prev_period_df.empty:
            prev_count = prev_period_df['Customer_ID'].nunique()
            if prev_count > 0:
                growth_rate = ((current_count - prev_count) / prev_count) * 100
        
        # Retention
        if prev_period_df is not None and not prev_period_df.empty:
            prev_customers = {cid for cid in prev_period_df['Customer_ID'].unique() if pd.notna(cid)}
            retained_customers = current_customers.intersection(prev_customers)
            retained_count = len(retained_customers)
            retention_pct = (retained_count / len(prev_customers) * 100) if len(prev_customers) > 0 else 0
        else:
            retained_count = 0
            retention_pct = 0
        
        return {
            'totalCustomers': int(current_count),
            'retainedCustomers': int(retained_count),
            'retentionPct': round(float(retention_pct), 2),
            'repeatCustomers': int(repeat_in_period),
            'repeatPct': round(float(repeat_pct), 2),
            'repeatCustomersTrx': int(repeat_trx_count),
            'repeatPctTrx': round(float(repeat_trx_pct), 2),
            'avgSpendPerCustomer': round(float(avg_spend), 2),
            'avgLifespan': round(float(avg_lifespan), 2),
            'growthRate': round(float(growth_rate), 2)
        }
    except Exception as e:
        raise Exception(f"Error calculating retention: {str(e)}")

def calculate_monthly_repeat_breakdown(df):
    """Calculate breakdown of repeat customers across months"""
    try:
        df_copy = df.copy()
        df_copy['YearMonth'] = df_copy['Date'].dt.to_period('M')
        df_copy['Visit_Date'] = df_copy['Date'].dt.date
        months = sorted(df_copy['YearMonth'].unique())
        
        monthly_breakdown = []
        
        for month in months:
            month_df = df_copy[df_copy['YearMonth'] == month]
            
            # Count unique visit days per customer in this month
            visit_days = month_df.groupby('Customer_ID')['Visit_Date'].nunique()
            
            # Breakdown by customer type
            one_timers_month = (visit_days == 1).sum()
            repeat_customers_month = (visit_days > 1).sum()
            total_customers_month = len(visit_days)
            
            # Calculate percentages
            repeat_pct_month = (repeat_customers_month / total_customers_month * 100) if total_customers_month > 0 else 0
            one_timer_pct_month = (one_timers_month / total_customers_month * 100) if total_customers_month > 0 else 0
            
            # Total revenue for the month
            total_revenue_month = month_df['Price'].sum()
            
            # Revenue from repeat customers
            repeat_customer_ids = visit_days[visit_days > 1].index
            repeat_revenue = month_df[month_df['Customer_ID'].isin(repeat_customer_ids)]['Price'].sum()
            repeat_revenue_pct = (repeat_revenue / total_revenue_month * 100) if total_revenue_month > 0 else 0
            
            monthly_breakdown.append({
                'period': str(month),
                'totalCustomers': int(total_customers_month),
                'oneTimers': int(one_timers_month),
                'oneTimerPct': round(float(one_timer_pct_month), 2),
                'repeatCustomers': int(repeat_customers_month),
                'repeatPct': round(float(repeat_pct_month), 2),
                'totalRevenue': round(float(total_revenue_month), 2),
                'repeatRevenue': round(float(repeat_revenue), 2),
                'repeatRevenuePct': round(float(repeat_revenue_pct), 2)
            })
        
        return monthly_breakdown
    except Exception as e:
        raise Exception(f"Error calculating monthly repeat breakdown: {str(e)}")

def calculate_semiannual_repeat_breakdown(df):
    """Calculate breakdown of repeat customers across semi-annual periods"""
    try:
        df_copy = df.copy()
        df_copy['Half'] = df_copy['Date'].dt.month.apply(lambda x: 'H1' if x <= 6 else 'H2')
        df_copy['YearHalf'] = df_copy['Date'].dt.year.astype(str) + '-' + df_copy['Half']
        df_copy['Visit_Date'] = df_copy['Date'].dt.date
        
        halves = sorted(df_copy['YearHalf'].unique())
        semi_annual_breakdown = []
        
        for half in halves:
            half_df = df_copy[df_copy['YearHalf'] == half]
            
            # Count unique visit days per customer in this half
            visit_days = half_df.groupby('Customer_ID')['Visit_Date'].nunique()
            
            # Breakdown by customer type
            one_timers_half = (visit_days == 1).sum()
            repeat_customers_half = (visit_days > 1).sum()
            total_customers_half = len(visit_days)
            
            # Calculate percentages
            repeat_pct_half = (repeat_customers_half / total_customers_half * 100) if total_customers_half > 0 else 0
            one_timer_pct_half = (one_timers_half / total_customers_half * 100) if total_customers_half > 0 else 0
            
            # Total revenue for the half
            total_revenue_half = half_df['Price'].sum()
            
            # Revenue from repeat customers
            repeat_customer_ids = visit_days[visit_days > 1].index
            repeat_revenue = half_df[half_df['Customer_ID'].isin(repeat_customer_ids)]['Price'].sum()
            repeat_revenue_pct = (repeat_revenue / total_revenue_half * 100) if total_revenue_half > 0 else 0
            
            semi_annual_breakdown.append({
                'period': half,
                'totalCustomers': int(total_customers_half),
                'oneTimers': int(one_timers_half),
                'oneTimerPct': round(float(one_timer_pct_half), 2),
                'repeatCustomers': int(repeat_customers_half),
                'repeatPct': round(float(repeat_pct_half), 2),
                'totalRevenue': round(float(total_revenue_half), 2),
                'repeatRevenue': round(float(repeat_revenue), 2),
                'repeatRevenuePct': round(float(repeat_revenue_pct), 2)
            })
        
        return semi_annual_breakdown
    except Exception as e:
        raise Exception(f"Error calculating semi-annual repeat breakdown: {str(e)}")

def calculate_overall_repeat_breakdown(df):
    """Calculate overall breakdown of repeat customers for the entire year"""
    try:
        df_copy = df.copy()
        df_copy['Visit_Date'] = df_copy['Date'].dt.date
        
        # Count unique visit days per customer for the entire year
        visit_days = df_copy.groupby('Customer_ID')['Visit_Date'].nunique()
        
        # Breakdown by customer type
        one_timers_overall = (visit_days == 1).sum()
        repeat_customers_overall = (visit_days > 1).sum()
        total_customers_overall = len(visit_days)
        
        # Calculate percentages
        repeat_pct_overall = (repeat_customers_overall / total_customers_overall * 100) if total_customers_overall > 0 else 0
        one_timer_pct_overall = (one_timers_overall / total_customers_overall * 100) if total_customers_overall > 0 else 0
        
        # Total revenue for the year
        total_revenue_overall = df_copy['Price'].sum()
        
        # Revenue from repeat customers
        repeat_customer_ids = visit_days[visit_days > 1].index
        repeat_revenue = df_copy[df_copy['Customer_ID'].isin(repeat_customer_ids)]['Price'].sum()
        repeat_revenue_pct = (repeat_revenue / total_revenue_overall * 100) if total_revenue_overall > 0 else 0
        
        # Additional metrics
        avg_transactions_per_customer = len(df_copy) / total_customers_overall if total_customers_overall > 0 else 0
        avg_spend_per_customer = df_copy.groupby('Customer_ID')['Price'].sum().mean()
        
        # Dynamic period based on data range
        min_year = df_copy['Date'].dt.year.min()
        max_year = df_copy['Date'].dt.year.max()
        period = f"{min_year}" if min_year == max_year else f"{min_year}-{max_year}"
        
        return {
            'period': period,
            'totalCustomers': int(total_customers_overall),
            'oneTimers': int(one_timers_overall),
            'oneTimerPct': round(float(one_timer_pct_overall), 2),
            'repeatCustomers': int(repeat_customers_overall),
            'repeatPct': round(float(repeat_pct_overall), 2),
            'totalRevenue': round(float(total_revenue_overall), 2),
            'repeatRevenue': round(float(repeat_revenue), 2),
            'repeatRevenuePct': round(float(repeat_revenue_pct), 2),
            'avgTransactionsPerCustomer': round(float(avg_transactions_per_customer), 2),
            'avgSpendPerCustomer': round(float(avg_spend_per_customer), 2)
        }
    except Exception as e:
        raise Exception(f"Error calculating overall repeat breakdown: {str(e)}")


def _calculate_trend_data(df_copy, period_column):
    """Generic function to calculate period-over-period trend data (Monthly, Quarterly, Semi-Annual, etc)"""
    try:
        periods = sorted(df_copy[period_column].unique())
        
        results = []
        all_seen_so_far = set()
        prev_period_df = None
        prev_result = None
        
        customer_min_date = {}
        customer_max_date = {}
        
        for period in periods:
            period_df = df_copy[df_copy[period_column] == period]
            # Get unique customers and ensure no NaN/Null values crash calculations
            current_customers = {cid for cid in period_df['Customer_ID'].unique() if pd.notna(cid)}
            current_count = len(current_customers)
            
            if current_count == 0:
                continue

            # 1. Retention (from previous period)
            retained_count = 0
            retention_pct = 0
            if prev_period_df is not None and not prev_period_df.empty:
                prev_customers = set(prev_period_df['Customer_ID'].unique())
                retained_customers = current_customers.intersection(prev_customers)
                retained_count = len(retained_customers)
                retention_pct = (retained_count / len(prev_customers) * 100) if len(prev_customers) > 0 else 0
                
            # 2. New Customers (First time seen in this dataset)
            new_customers = current_customers - all_seen_so_far
            new_count = len(new_customers)
            new_pct = (new_count / current_count * 100) if current_count > 0 else 0
            
            # 3. Repeat (Visit-day based)
            period_df_copy = period_df.copy()
            period_df_copy['Visit_Date'] = period_df_copy['Date'].dt.date
            visit_days = period_df_copy.groupby('Customer_ID')['Visit_Date'].nunique()
            repeat_count = (visit_days > 1).sum()
            repeat_pct = (repeat_count / current_count * 100) if current_count > 0 else 0
            
            # 4. Repeat Transaction Based
            trx_counts = period_df.groupby('Customer_ID').size()
            repeat_trx_count = (trx_counts > 1).sum()
            repeat_trx_pct = (repeat_trx_count / current_count * 100) if current_count > 0 else 0
            
            # 5. Average Spend
            total_revenue = period_df['Price'].sum()
            avg_spend = total_revenue / current_count if current_count > 0 else 0
            
            # 6. Average Lifespan (Cumulative up to this period)
            period_dates = period_df.groupby('Customer_ID')['Date'].agg(['min', 'max'])
            for cid, row in period_dates.iterrows():
                if cid not in customer_min_date:
                    customer_min_date[cid] = row['min']
                # Update max date if this row's latest date is newer
                if cid not in customer_max_date or row['max'] > customer_max_date[cid]:
                    customer_max_date[cid] = row['max']
                    
            lifespans = [(customer_max_date[cid] - customer_min_date[cid]).days for cid in customer_max_date]
            repeat_lifespans = [l for l in lifespans if l > 0]
            avg_lifespan = sum(repeat_lifespans) / len(repeat_lifespans) if repeat_lifespans else 0
            
            # Extract Revenue for backward compatibility with semi-annual logic
            repeat_customer_ids = visit_days[visit_days > 1].index
            repeat_revenue = period_df[period_df['Customer_ID'].isin(repeat_customer_ids)]['Price'].sum()
            repeat_revenue_pct = (repeat_revenue / total_revenue * 100) if total_revenue > 0 else 0
                
            result = {
                'period': str(period),
                'totalCustomers': int(current_count),
                'newCustomers': int(new_count),
                'newPct': round(float(new_pct), 2),
                'retainedCustomers': int(retained_count),
                'retentionPct': round(float(retention_pct), 2),
                'repeatCustomers': int(repeat_count),
                'repeatPct': round(float(repeat_pct), 2),
                'repeatCustomersTrx': int(repeat_trx_count),
                'repeatPctTrx': round(float(repeat_trx_pct), 2),
                'avgSpendPerCustomer': round(float(avg_spend), 2),
                'avgLifespan': round(float(avg_lifespan), 2),
                'growthRate': 0,  # Default for first period
                'marketingSpend': 0,
                'cac': 0,
                'totalRevenue': round(float(total_revenue), 2),
                'repeatRevenue': round(float(repeat_revenue), 2),
                'repeatRevenuePct': round(float(repeat_revenue_pct), 2)
            }
            
            # 7. CAC Calculation from Sheet Column
            if 'MARKETING EXPENSE' in period_df.columns:
                # Sum unique daily spends to avoid double counting if spend is recorded on every row
                marketing_cost = period_df.groupby(period_df['Date'].dt.date)['MARKETING EXPENSE'].max().sum()
                result['marketingSpend'] = float(marketing_cost)
                result['cac'] = round(marketing_cost / new_count, 2) if new_count > 0 else 0
            
            # Period-over-Period Growth & Comparative Values
            def get_growth(curr, prev):
                if prev is None or prev == 0: return 0
                return round(((curr - prev) / prev * 100), 2)

            if prev_result:
                result['growthRate'] = get_growth(result['totalCustomers'], prev_result['totalCustomers'])
                result['prev_repeatCustomers'] = prev_result['repeatCustomers']
                result['repeatGrowth'] = get_growth(result['repeatCustomers'], prev_result['repeatCustomers'])
                
                result['prev_avgSpend'] = prev_result['avgSpendPerCustomer']
                result['avgSpendGrowth'] = get_growth(result['avgSpendPerCustomer'], prev_result['avgSpendPerCustomer'])
                
                result['prev_avgLifespan'] = prev_result['avgLifespan']
                result['avgLifespanGrowth'] = get_growth(result['avgLifespan'], prev_result['avgLifespan'])
            else:
                result['prev_repeatCustomers'] = 0
                result['repeatGrowth'] = 0
                result['prev_avgSpend'] = 0
                result['avgSpendGrowth'] = 0
                result['prev_avgLifespan'] = 0
                result['avgLifespanGrowth'] = 0
            
            results.append(result)
            all_seen_so_far.update(current_customers)
            prev_period_df = period_df
            prev_result = result
            
        return results
    except Exception as e:
        raise Exception(f"Error calculating trend data: {str(e)}")

def calculate_monthly_data(df):
    """Calculate month-to-month metrics with MoM growth and lifecycle stats"""
    try:
        df_copy = df.copy()
        df_copy['YearMonth'] = df_copy['Date'].dt.to_period('M')
        return _calculate_trend_data(df_copy, 'YearMonth')
    except Exception as e:
        raise Exception(f"Error calculating monthly data: {str(e)}")


def calculate_cumulative_retention(df, start_date='2025-04-01'):
    """
    Calculate retention and repeat growth from a fixed start date.
    Tracks 'returning customers' and 'total repeaters' found so far.
    """
    try:
        df_copy = df.copy()
        df_copy['Date'] = pd.to_datetime(df_copy['Date'])
        df_copy['Visit_Date'] = df_copy['Date'].dt.date
        start_date_dt = pd.to_datetime(start_date)
        
        df_filtered = df_copy[df_copy['Date'] >= start_date_dt].copy()
        if df_filtered.empty: return []
            
        df_filtered['YearMonth'] = df_filtered['Date'].dt.to_period('M')
        months = sorted(df_filtered['YearMonth'].unique())
        
        cumulative_results = []
        seen_customer_visit_days = {} # Customer_ID -> set of Visit_Dates
        
        for i, month in enumerate(months):
            month_df = df_filtered[df_filtered['YearMonth'] == month]
            total_revenue = month_df['Price'].sum()
            
            # Customers in this month
            current_month_visits = month_df.groupby('Customer_ID')['Visit_Date'].unique().to_dict()
            current_customers = set(current_month_visits.keys())
            
            # Retained: Seen in ANY previous month
            all_seen_so_far = set(seen_customer_visit_days.keys())
            retained_customers = current_customers.intersection(all_seen_so_far)
            retained_count = len(retained_customers)
            retention_pct = (retained_count / len(current_customers) * 100) if len(current_customers) > 0 else 0
            
            # New: Not seen before
            new_customers = current_customers - all_seen_so_far
            new_count = len(new_customers)
            new_pct = (new_count / len(current_customers) * 100) if len(current_customers) > 0 else 0

            # Update master tracker
            for cid, vdays in current_month_visits.items():
                if cid not in seen_customer_visit_days:
                    seen_customer_visit_days[cid] = set()
                seen_customer_visit_days[cid].update(vdays)
            
            # Cumulative Stats
            total_unique_so_far = len(seen_customer_visit_days)
            repeaters_so_far = sum(1 for cid, vdays in seen_customer_visit_days.items() if len(vdays) > 1)
            cumulative_repeat_pct = (repeaters_so_far / total_unique_so_far * 100) if total_unique_so_far > 0 else 0
            
            retained_revenue = month_df[month_df['Customer_ID'].isin(retained_customers)]['Price'].sum()
            retained_revenue_pct = (retained_revenue / total_revenue * 100) if total_revenue > 0 else 0
            
            cumulative_results.append({
                'period': str(month),
                'totalCustomers': int(len(current_customers)),
                'retainedCustomers': int(retained_count),
                'retentionPct': round(float(retention_pct), 2),
                'newCustomers': int(new_count),
                'newPct': round(float(new_pct), 2),
                'totalRevenue': round(float(total_revenue), 2),
                'retainedRevenue': round(float(retained_revenue), 2),
                'retainedRevenuePct': round(float(retained_revenue_pct), 2),
                'cumulativeRepeatCount': int(repeaters_so_far),
                'cumulativeRepeatPct': round(float(cumulative_repeat_pct), 2),
                'cumulativeUniqueTotal': int(total_unique_so_far)
            })
            
        return cumulative_results
    except Exception as e:
        print(f"[ERROR] Error in calculate_cumulative_retention: {str(e)}")
        return []

def calculate_quarterly_data(df):
    """Calculate quarterly metrics"""
    try:
        df_copy = df.copy()
        df_copy['YearQuarter'] = df_copy['Date'].dt.to_period('Q')
        return _calculate_trend_data(df_copy, 'YearQuarter')
    except Exception as e:
        raise Exception(f"Error calculating quarterly data: {str(e)}")

def calculate_overall_performance(df):
    """Calculate overall performance metrics for the entire year"""
    try:
        df_copy = df.copy()
        df_copy['Visit_Date'] = df_copy['Date'].dt.date
        
        # Total metrics
        total_customers = df_copy['Customer_ID'].nunique()
        total_transactions = len(df_copy)
        total_revenue = df_copy['Price'].sum()
        
        # Visit metrics
        total_visits = len(df_copy.groupby(['Customer_ID', 'Visit_Date']))
        avg_spend_per_visit = df_copy.groupby(['Customer_ID', 'Visit_Date'])['Price'].sum().mean()
        
        # Customer types
        visit_days = df_copy.groupby('Customer_ID')['Visit_Date'].nunique()
        one_timers = (visit_days == 1).sum()
        repeat_customers = (visit_days > 1).sum()
        one_timer_pct = (one_timers / total_customers * 100) if total_customers > 0 else 0
        repeat_pct = (repeat_customers / total_customers * 100) if total_customers > 0 else 0
        
        # Revenue breakdown
        repeat_customer_ids = visit_days[visit_days > 1].index
        repeat_revenue = df_copy[df_copy['Customer_ID'].isin(repeat_customer_ids)]['Price'].sum()
        repeat_revenue_pct = (repeat_revenue / total_revenue * 100) if total_revenue > 0 else 0
        
        # Average customer metrics
        avg_spend_per_customer = df_copy.groupby('Customer_ID')['Price'].sum().mean()
        # Average lifespan (for repeat customers only)
        # Difference between first and last purchase date
        lifespan_df = df_copy.groupby('Customer_ID')['Date'].agg(['min', 'max'])
        lifespan_df['lifespan_days'] = (lifespan_df['max'] - lifespan_df['min']).dt.days
        repeat_customers_lifespan = lifespan_df[lifespan_df['lifespan_days'] > 0]
        avg_lifespan = repeat_customers_lifespan['lifespan_days'].mean() if len(repeat_customers_lifespan) > 0 else 0
        
        # Performance status
        repeat_status = '✓' if 20 <= repeat_pct <= 30 else '✗'
        
        # Calculate Marketing Spend and CAC
        marketing_cost = 0
        cac = 0
        if 'MARKETING EXPENSE' in df_copy.columns:
            marketing_cost = df_copy.groupby(df_copy['Date'].dt.date)['MARKETING EXPENSE'].max().sum()
            cac = marketing_cost / total_customers if total_customers > 0 else 0

        return {
            'totalCustomers': int(total_customers),
            'totalTransactions': int(total_transactions),
            'totalVisits': int(total_visits),
            'totalRevenue': round(float(total_revenue), 2),
            'avgSpendPerVisit': round(float(avg_spend_per_visit), 2) if not np.isnan(avg_spend_per_visit) else 0,
            'avgSpendPerCustomer': round(float(avg_spend_per_customer), 2) if not np.isnan(avg_spend_per_customer) else 0,
            'oneTimers': int(one_timers),
            'oneTimerPct': round(float(one_timer_pct), 2),
            'repeatCustomers': int(repeat_customers),
            'repeatPct': round(float(repeat_pct), 2),
            'repeatRevenue': round(float(repeat_revenue), 2),
            'repeatRevenuePct': round(float(repeat_revenue_pct), 2),
            'avgLifespan': round(float(avg_lifespan), 2) if not np.isnan(avg_lifespan) else 0,
            'performanceStatus': repeat_status,
            'marketingSpend': float(marketing_cost),
            'cac': float(cac)
        }
    except Exception as e:
        raise Exception(f"Error calculating overall performance: {str(e)}")

def calculate_semiannual_performance(df):
    """Calculate semi-annual metrics with revenue breakdown"""
    try:
        df_copy = df.copy()
        df_copy['Half'] = df_copy['Date'].dt.month.apply(lambda x: 'H1' if x <= 6 else 'H2')
        df_copy['YearHalf'] = df_copy['Date'].dt.year.astype(str) + '-' + df_copy['Half']
        return _calculate_trend_data(df_copy, 'YearHalf')
    except Exception as e:
        raise Exception(f"Error calculating semi-annual data: {str(e)}")

def calculate_yearly_data(df):
    """Calculate yearly metrics"""
    try:
        df_copy = df.copy()
        df_copy['Year'] = df_copy['Date'].dt.year.astype(str)
        return _calculate_trend_data(df_copy, 'Year')
    except Exception as e:
        raise Exception(f"Error calculating yearly data: {str(e)}")

def calculate_regional_data(df):
    """Calculate regional performance metrics"""
    try:
        if df is None or df.empty:
            return []
            
        regions = {}
        
        # Group shops by region - only include shops that exist in the data
        available_shops = df['Shop'].unique() if 'Shop' in df.columns else []
        
        for shop_name in available_shops:
            if shop_name in SHOP_REGION_MAP:
                region = SHOP_REGION_MAP[shop_name]
                if region not in regions:
                    regions[region] = []
                regions[region].append(shop_name)
        
        regional_results = []
        
        for region, shops_in_region in regions.items():
            # Filter data for shops in this region
            region_df = df[df['Shop'].isin(shops_in_region)]
            
            if region_df.empty:
                continue
            
            # Calculate aggregated metrics for the region
            region_metrics = calculate_overall_performance(region_df)
            region_overview = calculate_overview(region_df)
            
            # Add region-specific data
            regional_results.append({
                'region': region,
                'color': REGION_COLORS.get(region, '#95a5a6'),
                'shops': shops_in_region,
                'totalShops': len(shops_in_region),
                'totalCustomers': region_metrics['totalCustomers'],
                'repeatCustomers': region_metrics['repeatCustomers'],
                'repeatPct': region_metrics['repeatPct'],
                'totalRevenue': region_metrics['totalRevenue'],
                'repeatRevenuePct': region_metrics['repeatRevenuePct'],
                'avgSpendPerCustomer': region_metrics['avgSpendPerCustomer'],
                'performanceStatus': region_metrics['performanceStatus'],
                'oneTimerPct': region_overview['oneTimerPct'],
                'avgLifespan': region_overview['avgLifespan']
            })
        
        # Sort by total revenue descending
        regional_results.sort(key=lambda x: x['totalRevenue'], reverse=True)
        
        return regional_results
    except Exception as e:
        print(f"[ERROR] Error in calculate_regional_data: {str(e)}")
        return []

def calculate_gender_performance(df):
    """Calculate performance metrics by gender"""
    try:
        if 'Gender' not in df.columns:
            return []
        
        gender_results = []
        genders = df['Gender'].unique()
        
        for gender in genders:
            if pd.isna(gender) or gender == '':
                continue
                
            gender_df = df[df['Gender'] == gender]
            
            if gender_df.empty:
                continue
            
            # Calculate metrics
            total_customers = gender_df['Customer_ID'].nunique()
            total_revenue = gender_df['Price'].sum()
            avg_spend = total_revenue / total_customers if total_customers > 0 else 0
            total_transactions = len(gender_df)
            avg_transactions_per_customer = total_transactions / total_customers if total_customers > 0 else 0
            
            # Repeat customers
            visit_days = gender_df.groupby('Customer_ID')['Date'].nunique()
            repeat_customers = (visit_days > 1).sum()
            repeat_pct = (repeat_customers / total_customers * 100) if total_customers > 0 else 0
            
            gender_results.append({
                'gender': gender,
                'totalCustomers': int(total_customers),
                'totalRevenue': round(float(total_revenue), 2),
                'avgSpendPerCustomer': round(float(avg_spend), 2),
                'avgTransactionsPerCustomer': round(float(avg_transactions_per_customer), 2),
                'repeatCustomers': int(repeat_customers),
                'repeatPct': round(float(repeat_pct), 2),
                'totalTransactions': int(total_transactions)
            })
        
        return gender_results
    except Exception as e:
        print(f"[ERROR] Error in calculate_gender_performance: {str(e)}")
        return []

def calculate_product_performance(df):
    """Calculate top products by revenue"""
    try:
        # Check for product column - could be 'Product', 'Item', 'Product Name', etc.
        product_column = None
        possible_columns = ['Product', 'Item', 'Product Name', 'Item Name', 'ProductName', 'ItemName', 'Items', 'Description', 'Specifics']
        
        for col in possible_columns:
            if col in df.columns:
                product_column = col
                break
        
        if product_column is None:
            print("[WARNING] No product column found")
            return []
        
        # Group by product and calculate metrics
        product_stats = df.groupby(product_column).agg({
            'Price': ['sum', 'count', 'mean'],
            'Customer_ID': 'nunique'
        }).reset_index()
        
        product_stats.columns = [product_column, 'totalRevenue', 'totalSales', 'avgPrice', 'uniqueCustomers']
        
        # Sort by total revenue and get top 10
        top_products = product_stats.nlargest(10, 'totalRevenue')
        
        original_prices = {
            'JUMBO': 3600, 'MAN BAG': 1600, 'ANTITHEFT': 2600, 'CODE 3': 2100,
            'Standard Travel': 2600, 'SAFIRI TRAVEL': 3600, 'FABELA': 2600,
            'KAI': 2800, 'ELYSE': 3100, 'LOLA': 2100, 'MEGA': 3000, 'SCHOOL BAG': 2100
        }

        product_results = []
        for _, row in top_products.iterrows():
            product_name = row[product_column]
            # Fuzzy match or direct match for original price
            orig_price = original_prices.get(product_name, 0)
            avg_sold_price = row['avgPrice']
            
            # If explicit match not found, try case-insensitive or partial
            if orig_price == 0:
                for k, v in original_prices.items():
                    if k.lower() in str(product_name).lower():
                        orig_price = v
                        break
            
            discount_impact = 0
            if orig_price > 0:
                discount_impact = (orig_price - avg_sold_price) * row['totalSales']

            product_results.append({
                'product': product_name,
                'totalRevenue': round(float(row['totalRevenue']), 2),
                'totalSales': int(row['totalSales']),
                'avgPrice': round(float(row['avgPrice']), 2),
                'uniqueCustomers': int(row['uniqueCustomers']),
                'originalPrice': orig_price,
                'discountImpact': round(float(discount_impact), 2)
            })
        
        return product_results
    except Exception as e:
        print(f"[ERROR] Error in calculate_product_performance: {str(e)}")
        return []

def analyze_combos_and_affinity(df):
    """Analyze pre-defined combos (+) and market basket affinity"""
    try:
        # Check for product column
        product_column = None
        possible_columns = ['Product', 'Item', 'Product Name', 'Item Name', 'ProductName', 'ItemName', 'Items', 'Description', 'Specifics']
        for col in possible_columns:
            if col in df.columns:
                product_column = col
                break
        
        if not product_column: return {'combos': [], 'affinity': []}

        # 1. Analyze Existing Combos (with '+', 'Buy', 'Combo', 'Bundle')
        combo_keywords = r'(\+|buy\s+.*get\s+.*|combo|bundle|set)'
        combo_mask = df[product_column].astype(str).str.contains(combo_keywords, case=False, regex=True, na=False)
        combos_df = df[combo_mask]
        
        if not combos_df.empty:
            combo_stats = combos_df.groupby(product_column).agg({
                'Price': ['sum', 'count'],
                'Customer_ID': 'nunique'
            }).reset_index()
            combo_stats.columns = ['ComboName', 'Revenue', 'SalesCount', 'UniqueCustomers']
            top_combos = combo_stats.sort_values('Revenue', ascending=False).head(10).to_dict('records')
        else:
            top_combos = []

        # 2. Market Basket Analysis (Affinity)
        # Group by transaction (Customer + Date)
        df_copy = df.copy()
        df_copy['Visit_Date'] = df_copy['Date'].dt.date
        transactions = df_copy.groupby(['Customer_ID', 'Visit_Date'])[product_column].unique()
        
        # Filter for transactions with > 1 item
        multi_item_txns = transactions[transactions.apply(len) > 1]
        
        pair_counts = {}
        triplet_counts = {}
        import itertools

        for items in multi_item_txns:
            # Sort items to ensure (A, B) is same as (B, A)
            sorted_items = sorted([str(i) for i in items])
            
            # Generate pairs
            for pair in itertools.combinations(sorted_items, 2):
                if pair not in pair_counts: pair_counts[pair] = 0
                pair_counts[pair] += 1
            
            # Generate triplets
            for triplet in itertools.combinations(sorted_items, 3):
                if triplet not in triplet_counts: triplet_counts[triplet] = 0
                triplet_counts[triplet] += 1
        
        # Convert to list and sort
        affinity_results = []
        
        # Top Pairs
        for pair, count in sorted(pair_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
            affinity_results.append({
                'items': ' + '.join(pair),
                'type': 'Pair',
                'frequency': count
            })
            
        # Top Triplets
        for triplet, count in sorted(triplet_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
            affinity_results.append({
                'items': ' + '.join(triplet),
                'type': 'Triplet',
                'frequency': count
            })

        return {
            'topCombos': top_combos,
            'affinity': affinity_results
        }

    except Exception as e:
        print(f"[ERROR] Affinity Analysis: {str(e)}")
        import traceback
        traceback.print_exc()
        return {'combos': [], 'affinity': []}

def calculate_regional_top_products(df):
    """Get top 5 products per region"""
    try:
        # Product Col Check
        product_column = None
        for col in ['Product', 'Item', 'Product Name', 'Item Name', 'Items', 'Description']:
            if col in df.columns: product_column = col; break
        if not product_column: return {}

        regional_products = {}
        
        df_work = df.copy()
        # Ensure Region exists
        if 'Region' not in df_work.columns and 'Shop' in df_work.columns:
             df_work['Region'] = df_work['Shop'].map(SHOP_REGION_MAP)
        
        if 'Region' not in df_work.columns: return {}

        for region in df_work['Region'].unique():
            if not region or pd.isna(region): continue
            
            region_df = df_work[df_work['Region'] == region]
            
            # Group by Product Revenue
            stats = region_df.groupby(product_column)['Price'].sum().reset_index()
            top_5 = stats.sort_values('Price', ascending=False).head(5)
            
            products = []
            for _, row in top_5.iterrows():
                products.append({
                    'name': row[product_column],
                    'revenue': float(row['Price'])
                })
            
            regional_products[region] = products
            
        return regional_products

    except Exception as e:
        print(f"[ERROR] Regional Products: {str(e)}")
        return {}

def calculate_top_shops_by_region(df):
    """Calculate top 5 shops by revenue for each region"""
    try:
        if 'Shop' not in df.columns:
            return {}
        
        region_top_shops = {}
        
        # Get available shops and their regions
        available_shops = df['Shop'].unique()
        
        for shop in available_shops:
            if shop in SHOP_REGION_MAP:
                region = SHOP_REGION_MAP[shop]
                if region not in region_top_shops:
                    region_top_shops[region] = []
                
                shop_df = df[df['Shop'] == shop]
                total_revenue = shop_df['Price'].sum()
                total_customers = shop_df['Customer_ID'].nunique()
                total_transactions = len(shop_df)
                
                region_top_shops[region].append({
                    'shop': shop,
                    'totalRevenue': round(float(total_revenue), 2),
                    'totalCustomers': int(total_customers),
                    'totalTransactions': int(total_transactions),
                    'avgSpendPerCustomer': round(float(total_revenue / total_customers), 2) if total_customers > 0 else 0
                })
        
        # Sort and get top 5 for each region
        for region in region_top_shops:
            region_top_shops[region].sort(key=lambda x: x['totalRevenue'], reverse=True)
            region_top_shops[region] = region_top_shops[region][:5]
        
        return region_top_shops
    except Exception as e:
        print(f"[ERROR] Error in calculate_top_shops_by_region: {str(e)}")
        return {}

def calculate_monthly_loyalty_trends(df, target_shop):
    """Calculate month-over-month trends for Target-Only vs Cross-Shop customers"""
    try:
        if df.empty or 'Shop' not in df.columns:
            return []
            
        df_copy = df.copy()
        df_copy['YearMonth'] = df_copy['Date'].dt.to_period('M')
        months = sorted(df_copy['YearMonth'].unique())
        
        # Pre-identify all shops visited by each customer globally in this dataset
        # This determines if they are "Cross-Shop" or "Target-Only" once and for all
        cust_shops = df.groupby('Customer_ID')['Shop'].unique()
        is_target_only = cust_shops.apply(lambda x: len(x) == 1 and x[0] == target_shop)
        target_only_ids = set(is_target_only[is_target_only].index)
        
        trends = []
        for month in months:
            month_df = df_copy[df_copy['YearMonth'] == month]
            target_ids = set(month_df[month_df['Shop'] == target_shop]['Customer_ID'].unique())
            
            if not target_ids:
                continue
                
            total = len(target_ids)
            target_only = len(target_ids.intersection(target_only_ids))
            cross_shop = total - target_only
            
            trends.append({
                'month': str(month),
                'total': total,
                'targetOnly': target_only,
                'targetOnlyPct': round((target_only / total * 100), 1) if total > 0 else 0,
                'crossShop': cross_shop,
                'crossShopPct': round((cross_shop / total * 100), 1) if total > 0 else 0
            })
            
        return trends
    except Exception as e:
        print(f"[ERROR] In loyalty trends for {target_shop}: {e}")
        return []

def calculate_shop_loyalty_analysis(df, target_shop, full_df=None):
    """Classify target shop customers as new (Target-only) or existing (from overall database).
       Optimized using vectorized operations.
    """
    try:
        # Use full_df for global context if provided, else use df
        context_df = full_df if full_df is not None else df
        
        if 'Shop' not in context_df.columns:
            return {'error': 'Shop column not found', 'totalCustomers': 0}
            
        # 1. Identify all customers who visited the target shop in the CURRENT (filtered) df
        target_customer_ids = df[df['Shop'] == target_shop]['Customer_ID'].unique()
        if len(target_customer_ids) == 0:
            return {
                'totalCustomers': 0, 'newCustomers': 0, 'existingCustomers': 0,
                'sourceShops': [], 'sourceRegions': []
            }
        
        # 2. Filter main DF to ONLY these customers for faster processing
        customer_df = df[df['Customer_ID'].isin(target_customer_ids)].copy()
        
        # 3. Pre-calculate metrics per customer using groupby
        # Find which shops each customer has visited
        cust_shops = customer_df.groupby('Customer_ID')['Shop'].unique()
        # Find which customers are "Target Only" (only 1 shop and it's the target)
        is_new = cust_shops.apply(lambda x: len(x) == 1 and x[0] == target_shop)
        
        new_ids = is_new[is_new].index
        existing_ids = is_new[~is_new].index
        
        # 4. Details for New Customers
        new_df = customer_df[customer_df['Customer_ID'].isin(new_ids)]
        new_stats = new_df.groupby('Customer_ID').agg(
            totalPurchases=('Price', 'count'),
            totalRevenue=('Price', 'sum'),
            firstPurchaseDate=('Date', 'min')
        ).reset_index()
        
        # 5. Details for Existing Customers (Cross-shoppers)
        existing_df = customer_df[customer_df['Customer_ID'].isin(existing_ids)]
        
        # Find other shops visited by these customers
        # Get all (Customer, Shop) pairs where Shop != target
        other_visits = existing_df[existing_df['Shop'] != target_shop][['Customer_ID', 'Shop']].drop_duplicates()
        source_shop_counts = other_visits['Shop'].value_counts()
        
        # Region counts
        other_visits['Region'] = other_visits['Shop'].map(lambda x: SHOP_REGION_MAP.get(x, 'Unknown'))
        source_region_counts = other_visits['Region'].value_counts()
        
        # Revenue in target shop for cross-shoppers
        target_rev_existing = existing_df[existing_df['Shop'] == target_shop].groupby('Customer_ID')['Price'].sum()
        
        # First shop visited (absolute first in database)
        first_visits = existing_df.sort_values('Date').groupby('Customer_ID').first()[['Shop']]
        
        # 6. Format Results
        source_shops = [{'shop': s, 'customerCount': int(c), 
                        'percentage': round((c/max(1, len(existing_ids))*100), 2)} 
                        for s, c in source_shop_counts.items()]
        
        source_regions = [{'region': r, 'customerCount': int(c), 
                          'percentage': round((c/max(1, len(existing_ids))*100), 2)} 
                          for r, c in source_region_counts.items()]
        
        # Prepare sample details (top 10 by revenue for new, random/first 10 for existing)
        new_details = []
        for _, row in new_stats.sort_values('totalRevenue', ascending=False).head(10).iterrows():
            new_details.append({
                'customerId': row['Customer_ID'],
                'totalPurchases': int(row['totalPurchases']),
                'totalRevenue': round(float(row['totalRevenue']), 2),
                'firstPurchaseDate': row['firstPurchaseDate'].strftime('%Y-%m-%d') if pd.notna(row['firstPurchaseDate']) else 'N/A'
            })
            
        existing_details = []
        # Get unique shops visited per existing customer (other than target)
        other_shops_per_cust = other_visits.groupby('Customer_ID')['Shop'].apply(list)
        
        for cid in existing_ids[:10]:
            target_rev = float(target_rev_existing.get(cid, 0))
            first_shop = first_visits.loc[cid, 'Shop']
            other_s = other_shops_per_cust.get(cid, [])
            existing_details.append({
                'customerId': cid,
                'otherShops': other_s,
                'firstShopVisited': first_shop,
                'targetRevenue': round(target_rev, 2)
            })
        
        return {
            'targetShop': target_shop,
            'totalCustomers': int(len(target_customer_ids)),
            'newCustomers': int(len(new_ids)),
            'newCustomerPercentage': round((len(new_ids)/len(target_customer_ids)*100), 2) if len(target_customer_ids) > 0 else 0,
            'existingCustomers': int(len(existing_ids)),
            'existingCustomerPercentage': round((len(existing_ids)/len(target_customer_ids)*100), 2) if len(target_customer_ids) > 0 else 0,
            'revenueFromNew': round(float(new_stats['totalRevenue'].sum()), 2) if not new_stats.empty else 0,
            'revenueFromExisting': round(float(target_rev_existing.sum()), 2) if not target_rev_existing.empty else 0,
            'sourceShops': source_shops,
            'sourceRegions': source_regions,
            'newCustomerDetails': new_details,
            'existingCustomerDetails': existing_details,
            'monthlyTrends': calculate_monthly_loyalty_trends(df, target_shop)
        }
    except Exception as e:
        import traceback
        print(f"[ERROR] In loyalty analysis for {target_shop}: {e}")
        print(traceback.format_exc())
        return {'error': str(e)}

        return {'error': str(e)}

def calculate_monthly_shop_overview(df):
    """Calculate monthly customer and units sold overview by shop using vectorized groupby."""
    try:
        empty_res = {
            'customers': {'shops': {}, 'totals': {}, 'months': []},
            'units': {'shops': {}, 'totals': {}, 'months': []},
            'products': {'months': [], 'products': {}},
            'debug': {'error': 'No data or Shop column missing'}
        }
        if df.empty or 'Shop' not in df.columns:
            return empty_res
        
        df_copy = df.copy()
        df_copy['YearMonth'] = df_copy['Date'].dt.to_period('M').astype(str)
        
        # Filter to relevant shops
        df_copy = df_copy[df_copy['Shop'].isin(SHOP_REGION_MAP.keys())]
        if df_copy.empty:
            return empty_res

        shops = sorted(df_copy['Shop'].unique())
        months = sorted(df_copy['YearMonth'].unique())
        
        # 1. Calculate Customers and Units per (Shop, Month) in one go
        grouped = df_copy.groupby(['Shop', 'YearMonth']).agg(
            unique_customers=('Customer_ID', 'nunique'),
            units_sold=('Customer_ID', 'count')
        ).unstack(fill_value=0)
        
        customer_data = grouped['unique_customers'].to_dict(orient='index')
        units_data = grouped['units_sold'].to_dict(orient='index')
        
        # Calculate totals per month
        monthly_grouped = df_copy.groupby('YearMonth').agg(
            unique_customers=('Customer_ID', 'nunique'),
            units_sold=('Customer_ID', 'count')
        )
        customer_totals = monthly_grouped['unique_customers'].to_dict()
        units_totals = monthly_grouped['units_sold'].to_dict()
        
        # 2. Product-wise Monthly Overview (Top 20 products)
        product_col = None
        for col in ['Product', 'Item', 'Product Name', 'Item Name', 'Items', 'Description', 'Specifics', 'Product_Name', 'ProductName', 'Items Purchased']:
            if col in df_copy.columns: product_col = col; break
            
        product_overview = {'months': months, 'products': {}}
        if product_col:
            # Get top 20 products by revenue overall
            top_products = df_copy.groupby(product_col)['Price'].sum().sort_values(ascending=False).head(20).index.tolist()
            
            # Map products to months
            prod_monthly = df_copy[df_copy[product_col].isin(top_products)].groupby([product_col, 'YearMonth'])['Customer_ID'].count().unstack(fill_value=0)
            product_overview['products'] = prod_monthly.to_dict(orient='index')
            
        return {
            'customers': {'shops': customer_data, 'totals': customer_totals, 'months': months},
            'units': {'shops': units_data, 'totals': units_totals, 'months': months},
            'products': product_overview
        }
    except Exception as e:
        print(f"[ERROR] Monthly Shop Overview: {str(e)}")
        return empty_res

def calculate_inactive_customers(df, days_threshold=30, last_month=None, last_year=None):
    """Identify customers who haven't made a purchase within the threshold days.
    Uses the latest date in the entire dataset as the reference point for 'today'.
    """
    try:
        if df.empty:
            return []
            
        # 1. Get latest date in dataset
        reference_date = df['Date'].max()
        
        # 2. Get last purchase date, first name, phone, and total spend per customer
        cust_stats = df.groupby('Customer_ID').agg(
            lastPurchaseDate=('Date', 'max'),
            firstName=('First Name', 'first'),
            phone=('Phone', 'first'),
            gender=('Gender', 'first'),
            totalSpend=('Price', 'sum'),
            totalVisits=('Date', 'nunique'),
            lastShop=('Shop', 'last')
        ).reset_index()
        
        # 3. Calculate inactivity period
        cust_stats['daysInactive'] = (reference_date - cust_stats['lastPurchaseDate']).dt.days
        
        # 4. Filter inactive logic
        if last_month and last_year and last_month != 'all' and last_year != 'all':
            # Specific cohort: people whose last purchase was exactly in this month/year
            inactive = cust_stats[
                (cust_stats['lastPurchaseDate'].dt.month == int(last_month)) &
                (cust_stats['lastPurchaseDate'].dt.year == int(last_year))
            ].copy()
        else:
            # Default: anyone who hasn't shopped for X days
            inactive = cust_stats[cust_stats['daysInactive'] >= days_threshold].copy()
        
        # 5. Sort by spend then days inactive
        inactive = inactive.sort_values(by=['totalSpend', 'daysInactive'], ascending=[False, False])
        
        # 6. Format results
        results = []
        for _, row in inactive.iterrows():
            results.append({
                'customerId': row['Customer_ID'],
                'firstName': str(row['firstName']),
                'phone': str(row['phone']),
                'gender': str(row['gender']),
                'totalSpend': round(float(row['totalSpend']), 2),
                'totalVisits': int(row['totalVisits']),
                'lastPurchaseDate': row['lastPurchaseDate'].strftime('%Y-%m-%d'),
                'daysInactive': int(row['daysInactive']),
                'lastShop': str(row['lastShop'])
            })
            
        return results
    except Exception as e:
        print(f"[ERROR] calculate_inactive_customers: {str(e)}")
        return []

def calculate_growth_rates(df):
    """Calculate full historical growth rates for customers for Monthly, Quarterly, and Yearly periods"""
    try:
        if df.empty or 'Customer_ID' not in df.columns:
            return {'monthly': [], 'quarterly': [], 'yearly': []}
            
        df_copy = df.copy()
        if 'Shop' in df_copy.columns:
            # Filter to only include shops in our map for consistency with other metrics
            df_copy = df_copy[df_copy['Shop'].isin(SHOP_REGION_MAP.keys())]
            
        if df_copy.empty:
            return {'monthly': [], 'quarterly': [], 'yearly': []}

        df_copy['Date'] = pd.to_datetime(df_copy['Date'])
        
        def compute_series(stats):
            series = []
            for i in range(len(stats)):
                curr_val = stats.iloc[i]['customers']
                prev_val = stats.iloc[i-1]['customers'] if i > 0 else 0
                
                growth = 0
                if i > 0 and prev_val > 0:
                    growth = round(((curr_val - prev_val) / prev_val * 100), 2)
                
                series.append({
                    'period': str(stats.index[i]),
                    'current': int(curr_val),
                    'previous': int(prev_val),
                    'growth': growth,
                    'is_start': i == 0
                })
            return series

        # 1. Monthly Series
        df_copy['Period_M'] = df_copy['Date'].dt.to_period('M')
        m_stats = df_copy.groupby('Period_M').agg({'Customer_ID': 'nunique'}).rename(columns={'Customer_ID': 'customers'}).sort_index()
        
        # 2. Quarterly Series
        df_copy['Period_Q'] = df_copy['Date'].dt.to_period('Q')
        q_stats = df_copy.groupby('Period_Q').agg({'Customer_ID': 'nunique'}).rename(columns={'Customer_ID': 'customers'}).sort_index()
        
        # 3. Yearly Series
        df_copy['Period_Y'] = df_copy['Date'].dt.to_period('Y')
        y_stats = df_copy.groupby('Period_Y').agg({'Customer_ID': 'nunique'}).rename(columns={'Customer_ID': 'customers'}).sort_index()

        return {
            'monthly': compute_series(m_stats),
            'quarterly': compute_series(q_stats),
            'yearly': compute_series(y_stats)
        }
        
    except Exception as e:
        print(f"[ERROR] Error in calculate_growth_rates: {str(e)}")
        return {'monthly': [], 'quarterly': [], 'yearly': []}
        
    except Exception as e:
        print(f"[ERROR] Error in calculate_growth_rates: {str(e)}")
        return []

@app.route('/')
def index():
    """Render the dashboard"""
    return render_template('index.html')

def _compute_all_results(df):
    """Compute all dashboard metrics. Called both by /api/data and the startup pre-warmer."""
    global computed_results_cache

    data = {}
    shops = {}
    if 'Shop' in df.columns:
        available_shops = df['Shop'].unique()
        for shop in available_shops:
            if shop in SHOP_REGION_MAP:
                shop_df = df[df['Shop'] == shop]
                if not shop_df.empty:
                    shops[shop] = {
                        'overall': calculate_overall_performance(shop_df),
                        'overallBreakdown': calculate_overall_repeat_breakdown(shop_df),
                        'overview': calculate_overview(shop_df),
                        'monthly': calculate_monthly_data(shop_df),
                        'monthlyRepeatBreakdown': calculate_monthly_repeat_breakdown(shop_df),
                        'quarterly': calculate_quarterly_data(shop_df),
                        'semiAnnual': calculate_semiannual_performance(shop_df),
                        'semiAnnualBreakdown': calculate_semiannual_repeat_breakdown(shop_df),
                        'yearly': calculate_yearly_data(shop_df),
                        'visitIntervals': calculate_visit_interval_distribution(shop_df),
                        'growthRates': calculate_growth_rates(shop_df)
                    }
    data['shops'] = shops

    overall_results = {
        'overall': calculate_overall_performance(df),
        'overallBreakdown': calculate_overall_repeat_breakdown(df),
        'overview': calculate_overview(df),
        'monthly': calculate_monthly_data(df),
        'monthlyRepeatBreakdown': calculate_monthly_repeat_breakdown(df),
        'quarterly': calculate_quarterly_data(df),
        'semiAnnual': calculate_semiannual_performance(df),
        'semiAnnualBreakdown': calculate_semiannual_repeat_breakdown(df),
        'yearly': calculate_yearly_data(df),
        'regions': calculate_regional_data(df),
        'gender': calculate_gender_performance(df),
        'products': calculate_product_performance(df),
        'topShopsByRegion': calculate_top_shops_by_region(df),
        'ktdaAnalysis': calculate_shop_loyalty_analysis(df, 'Ktda'),
        'rejectsAnalysis': calculate_shop_loyalty_analysis(df, 'Rejects'),
        'kisiiAnalysis': calculate_shop_loyalty_analysis(df, 'Kisii'),
        'hiltonAnalysis': calculate_shop_loyalty_analysis(df, 'Hilton'),
        'cumulativeRetention': calculate_cumulative_retention(df),
        'visitIntervals': calculate_visit_interval_distribution(df),
        'advancedProducts': analyze_combos_and_affinity(df),
        'regionalProducts': calculate_regional_top_products(df),
        'monthlyShopOverview': calculate_monthly_shop_overview(df),
        'growthRates': calculate_growth_rates(df)
    }
    data.update(overall_results)
    computed_results_cache = data
    return data

@app.route('/api/data')
def get_data():
    """API endpoint to get all dashboard data"""
    global computed_results_cache
    
    # Get filters from query parameters
    filter_year = request.args.get('year')
    filter_month = request.args.get('month')
    filter_quarter = request.args.get('quarter')
    filter_half = request.args.get('half')
    
    # Check if any filter is actually applied (not 'all' and not None)
    is_filtered = any([
        filter_year and filter_year != 'all',
        filter_month and filter_month != 'all',
        filter_quarter and filter_quarter != 'all',
        filter_half and filter_half != 'all'
    ])
    
    try:
        # 1. Fetch data
        df = get_customer_data()
        
        # 2. Apply time filters if provided
        if is_filtered:
            print(f"[INFO] Applying time filters: year={filter_year}, month={filter_month}, quarter={filter_quarter}")
            df_work = df.copy()
            
            if filter_year and filter_year != 'all':
                df_work = df_work[df_work['Date'].dt.year == int(filter_year)]
            
            if filter_month and filter_month != 'all':
                df_work = df_work[df_work['Date'].dt.month == int(filter_month)]
                
            if filter_quarter and filter_quarter != 'all':
                # Remove 'Q' if present (e.g., 'Q1' -> 1)
                q_val = filter_quarter.replace('Q', '')
                df_work = df_work[df_work['Date'].dt.quarter == int(q_val)]
            
            if filter_half and filter_half != 'all':
                if filter_half == 'H1':
                    df_work = df_work[df_work['Date'].dt.month <= 6]
                else:
                    df_work = df_work[df_work['Date'].dt.month > 6]
                
            df = df_work
        
        # 3. Handle caching for unfiltered requests only
        if not is_filtered and computed_results_cache is not None:
            computed_results_cache['cache_status'] = {
                'last_updated': datetime.fromtimestamp(last_fetch_time).strftime('%Y-%m-%d %H:%M:%S') if last_fetch_time else "Unknown",
                'type': 'computed_memory'
            }
            return jsonify(computed_results_cache)
            
        print("[INFO] Calculating metrics...")
        
        result = _compute_all_results(df)

        # SPECIAL: Inject CAC for the current filtered period if it's a single month
        if filter_month and filter_month != 'all' and filter_year and filter_year != 'all':
            month_key = f"{filter_year}-{int(filter_month):02d}"
            full_df = get_customer_data()
            full_monthly = calculate_monthly_data(full_df)
            matching = next((m for m in full_monthly if m['period'] == month_key), None)
            if matching and matching.get('marketingSpend', 0) > 0:
                if 'overview' in result:
                    result['overview']['cac'] = matching['cac']
                    result['overview']['marketingSpend'] = matching['marketingSpend']
                if 'overall' in result:
                    result['overall']['cac'] = matching['cac']
                    result['overall']['marketingSpend'] = matching['marketingSpend']
        
        # Save to computed cache ONLY if this is an unfiltered request
        if not is_filtered:
            computed_results_cache = result
        
        result['cache_status'] = {
            'last_updated': datetime.fromtimestamp(last_fetch_time).strftime('%Y-%m-%d %H:%M:%S') if last_fetch_time else "Unknown",
            'type': 'computed_fresh'
        }
        
        return jsonify(result)
    
    except Exception as e:
        import traceback
        print(f"[ERROR] in /api/data: {str(e)}")
        print(f"[TRACEBACK] {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500



@app.route('/api/ktda-customer-analysis')
def get_ktda_customer_analysis():
    """API endpoint to get KTDA customer classification analysis"""
    try:
        df = get_customer_data()
        analysis = calculate_shop_loyalty_analysis(df, 'Ktda')
        return jsonify(analysis)
    except Exception as e:
        import traceback
        print(f"[ERROR] in /api/ktda-customer-analysis: {str(e)}")
        print(f"[TRACEBACK] {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/inactive-customers')
def get_inactive_customers():
    """API endpoint to get list of inactive customers based on threshold or cohort month"""
    try:
        days = request.args.get('days', 30, type=int)
        last_month = request.args.get('month')
        last_year = request.args.get('year')
        
        df = get_customer_data()
        inactive = calculate_inactive_customers(df, days, last_month, last_year)
        return jsonify(inactive)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/export/inactive-customers')
def export_inactive_customers():
    """Export inactive customers as CSV"""
    try:
        from flask import Response
        days = request.args.get('days', 30, type=int)
        last_month = request.args.get('month')
        last_year = request.args.get('year')
        
        df = get_customer_data()
        inactive_list = calculate_inactive_customers(df, days, last_month, last_year)
        
        if not inactive_list:
             return Response("First Name,Phone,Gender,Last Purchase,Days Inactive,Total Spend,Total Visits,Last Shop\n", 
                            mimetype='text/csv')
        
        export_df = pd.DataFrame(inactive_list)
        # Rename for export
        export_df = export_df.rename(columns={
            'firstName': 'First Name',
            'phone': 'Phone',
            'gender': 'Gender',
            'lastPurchaseDate': 'Last Purchase',
            'daysInactive': 'Days Inactive',
            'totalSpend': 'Total Spend',
            'totalVisits': 'Total Visits',
            'lastShop': 'Last Shop'
        })
        
        # Reorder
        cols = ['First Name', 'Phone', 'Gender', 'Last Purchase', 'Days Inactive', 'Total Spend', 'Total Visits', 'Last Shop']
        export_df = export_df[cols]
        
        csv_output = export_df.to_csv(index=False)
        filename = f"inactive_customers_{days}_days.csv"
        
        return Response(csv_output, mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={filename}', 'Content-Type': 'text/csv; charset=utf-8'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/refresh-now', methods=['POST'])
def refresh_now():
    """Force refresh data from Google Sheets"""
    global cached_data, last_fetch_time, computed_results_cache
    cached_data = None
    last_fetch_time = None
    computed_results_cache = None  # Clear computed cache
    
    # Remove persistent cache file to force fresh fetch
    if os.path.exists(CACHE_FILE):
        try:
            os.remove(CACHE_FILE)
            print(f"[INFO] Removed persistent cache file: {CACHE_FILE}")
        except Exception as e:
            print(f"[WARNING] Could not remove persistent cache: {e}")
            
    return get_data()

@app.route('/api/export/repeat-customers')
def export_repeat_customers():
    """Export all transaction rows for retained (repeat) customers, including products.
    Optional query param: ?shop=<ShopName>
    """
    try:
        from flask import Response
        df = get_customer_data()

        # Optional shop filter
        shop_filter = request.args.get('shop', '').strip()
        if shop_filter and shop_filter in df['Shop'].values:
            df_work = df[df['Shop'] == shop_filter].copy()
            filename = f"repeat_customers_{shop_filter.lower().replace(' ', '_')}.csv"
        else:
            df_work = df.copy()
            filename = "repeat_customers_all.csv"

        # Identify repeat IDs (visited on at least 2 different calendar days)
        df_work['Visit_Date'] = df_work['Date'].dt.date
        visit_days = df_work.groupby('Customer_ID')['Visit_Date'].nunique()
        repeat_ids = visit_days[visit_days >= 2].index

        # Filter for the target customers
        # IMPORTANT: We use the full original row set for these customers
        export_df = df_work[df_work['Customer_ID'].isin(repeat_ids)].copy()

        if export_df.empty:
            return Response("First Name,Phone,Gender,Shop,Date,Product,Price,Total_Customer_Spend\n", 
                            mimetype='text/csv', headers={'Content-Disposition': f'attachment; filename={filename}'})

        # Calculate total spend per customer for sorting
        spend_map = export_df.groupby('Customer_ID')['Price'].sum().to_dict()
        export_df['Total_Customer_Spend'] = export_df['Customer_ID'].map(spend_map)

        # Select and reorder columns for the call center
        cols = ['First Name', 'Phone', 'Gender', 'Shop', 'Date', 'Product', 'Price', 'Total_Customer_Spend', 'Customer_ID']
        # Check matching columns
        existing_cols = [c for c in cols if c in export_df.columns]
        export_df = export_df[existing_cols]

        # Sort by Customer Spend (Descending) then Date
        export_df = export_df.sort_values(by=['Total_Customer_Spend', 'Customer_ID', 'Date'], ascending=[False, True, False])
        
        # Now drop Customer_ID if we don't want it in the final CSV
        if 'Customer_ID' in export_df.columns:
            export_df = export_df.drop(columns=['Customer_ID'])

        csv_output = export_df.to_csv(index=False)
        print(f"[INFO] Exporting {len(export_df)} transaction rows for {len(repeat_ids)} repeat customers")
        
        return Response(csv_output, mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={filename}', 'Content-Type': 'text/csv; charset=utf-8'})

    except Exception as e:
        import traceback
        print(f"[ERROR] export_repeat_customers: {str(e)}")
        print(f"[TRACEBACK] {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/export/one-time-customers')
def export_one_time_customers():
    """Export all transaction rows for one-time (single-visit) customers, including products.
    Optional query param: ?shop=<ShopName>
    """
    try:
        from flask import Response
        df = get_customer_data()

        # Optional shop filter
        shop_filter = request.args.get('shop', '').strip()
        if shop_filter and shop_filter in df['Shop'].values:
            df_work = df[df['Shop'] == shop_filter].copy()
            filename = f"one_time_customers_{shop_filter.lower().replace(' ', '_')}.csv"
        else:
            df_work = df.copy()
            filename = "one_time_customers_all.csv"

        # Identify one-time IDs (visited on exactly 1 unique calendar day)
        df_work['Visit_Date'] = df_work['Date'].dt.date
        visit_days = df_work.groupby('Customer_ID')['Visit_Date'].nunique()
        one_time_ids = visit_days[visit_days == 1].index

        # Filter for the target customers
        export_df = df_work[df_work['Customer_ID'].isin(one_time_ids)].copy()

        if export_df.empty:
            return Response("First Name,Phone,Gender,Shop,Date,Product,Price,Total_Customer_Spend\n", 
                            mimetype='text/csv', headers={'Content-Disposition': f'attachment; filename={filename}'})

        # Calculate total spend per customer for sorting
        spend_map = export_df.groupby('Customer_ID')['Price'].sum().to_dict()
        export_df['Total_Customer_Spend'] = export_df['Customer_ID'].map(spend_map)

        # Select relevant columns
        cols = ['First Name', 'Phone', 'Gender', 'Shop', 'Date', 'Product', 'Price', 'Total_Customer_Spend', 'Customer_ID']
        existing_cols = [c for c in cols if c in export_df.columns]
        export_df = export_df[existing_cols]

        # Sort by Customer Spend (Descending)
        export_df = export_df.sort_values(by=['Total_Customer_Spend', 'Customer_ID'], ascending=[False, True])
        
        # Drop Customer_ID for final output
        if 'Customer_ID' in export_df.columns:
            export_df = export_df.drop(columns=['Customer_ID'])

        csv_output = export_df.to_csv(index=False)
        print(f"[INFO] Exporting {len(export_df)} transaction rows for {len(one_time_ids)} one-time customers")
        
        return Response(csv_output, mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={filename}', 'Content-Type': 'text/csv; charset=utf-8'})

    except Exception as e:
        import traceback
        print(f"[ERROR] export_one_time_customers: {str(e)}")
        print(f"[TRACEBACK] {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/upload', methods=['POST'])
def upload_data():
    """Upload CSV data and append to Google Sheets"""
    global cached_data, last_fetch_time
    
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        if not file.filename.endswith('.csv'):
            return jsonify({'error': 'Only CSV files are allowed'}), 400
        
        # Read CSV data
        csv_data = file.read().decode('utf-8')
        df_upload = pd.read_csv(io.StringIO(csv_data))
        
        # Validate required columns
        required_columns = ['Date', 'First Name', 'Phone', 'Price', 'Shop']
        missing_columns = [col for col in required_columns if col not in df_upload.columns]
        if missing_columns:
            return jsonify({'error': f'Missing required columns: {", ".join(missing_columns)}'}), 400
        
        # Setup Google Sheets authentication
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 
                  'https://www.googleapis.com/auth/drive']
        
        if os.path.exists(JSON_FILE_PATH):
            # Local development
            creds = Credentials.from_service_account_file(JSON_FILE_PATH, scopes=SCOPES)
        elif GOOGLE_SERVICE_ACCOUNT_JSON:
            # Render / cloud deployment
            creds_info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
            creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
        else:
            raise ValueError("No Google credentials found.")
        client = gspread.authorize(creds)
        
        # Open the spreadsheet and worksheet
        spreadsheet = client.open(SHEET_NAME)
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
        
        # Convert DataFrame to list of lists for gspread
        # Get existing headers
        existing_data = worksheet.get_all_records()
        if existing_data:
            # Append new data
            new_rows = df_upload.values.tolist()
            worksheet.append_rows(new_rows)
        else:
            # If sheet is empty, include headers
            headers = df_upload.columns.tolist()
            all_data = [headers] + df_upload.values.tolist()
            worksheet.update(all_data)
        
        # Clear cache to force refresh
        cached_data = None
        last_fetch_time = None
        computed_results_cache = None # Clear computed cache
        
        # Remove persistent cache file
        if os.path.exists(CACHE_FILE):
            try:
                os.remove(CACHE_FILE)
                print(f"[INFO] Removed persistent cache file after upload: {CACHE_FILE}")
            except Exception as e:
                print(f"[WARNING] Could not remove persistent cache: {e}")
        
        return jsonify({
            'success': True, 
            'message': f'Successfully uploaded {len(df_upload)} records',
            'records_uploaded': len(df_upload)
        })
    
    except Exception as e:
        import traceback
        print(f"[ERROR] Upload failed: {str(e)}")
        print(f"[TRACEBACK] {traceback.format_exc()}")
        return jsonify({'error': f'Upload failed: {str(e)}'}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5002))
    app.run(host="0.0.0.0", port=port)
else:
    # When launched by gunicorn, pre-warm the cache in a background thread.
    # This means by the time the first browser request arrives, all the heavy
    # computation is already done and results are served instantly from cache.
    def _prewarm():
        try:
            print("[INFO] Pre-warming data cache in background thread...")
            with app.app_context():
                from flask import Request
                with app.test_request_context('/'):
                    import importlib, sys
                    # Directly call the internal compute path
                    df = get_customer_data()
                    if df is not None and not df.empty:
                        _compute_all_results(df)
                        print("[INFO] Pre-warm complete. Data is ready.")
        except Exception as e:
            print(f"[WARNING] Pre-warm failed (non-fatal): {e}")

    threading.Thread(target=_prewarm, daemon=True).start()