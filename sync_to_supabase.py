"""
Standalone script: pull data from Google Sheets → push to Supabase.
Run from the project root:  python sync_to_supabase.py
"""

import os, json, sys
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://oeluiwinbzlmjsbtjlfq.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
SHEET_NAME   = os.environ.get("SHEET_NAME", "Customer Database")
WORKSHEET    = os.environ.get("WORKSHEET_NAME", "Shops")
JSON_FILE    = os.environ.get("JSON_FILE_PATH",
               r"C:\Users\Oduor\Downloads\JSON Files\retention-484110-9e4520124486.json")
GOOGLE_JSON  = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
TABLE        = "sales"
BATCH        = 500

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# ── Step 1: validate credentials ──────────────────────────────────────────────
print("\n=== STEP 1: Checking credentials ===")
if not SUPABASE_KEY:
    sys.exit("ERROR: SUPABASE_KEY not set. Check your .env file.")
print(f"  Supabase URL : {SUPABASE_URL}")
print(f"  Supabase key : {SUPABASE_KEY[:30]}...")
print("  OK")

# ── Step 2: connect to Google Sheets ─────────────────────────────────────────
print("\n=== STEP 2: Loading data from Google Sheets ===")
try:
    import gspread
    from google.oauth2.service_account import Credentials

    if os.path.exists(JSON_FILE):
        creds = Credentials.from_service_account_file(JSON_FILE, scopes=SCOPES)
        print(f"  Using local JSON file: {JSON_FILE}")
    elif GOOGLE_JSON:
        raw = GOOGLE_JSON.strip()
        try:
            info = json.loads(raw)
        except json.JSONDecodeError:
            import base64
            info = json.loads(base64.b64decode(raw).decode())
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
        print("  Using GOOGLE_SERVICE_ACCOUNT_JSON env var")
    else:
        sys.exit("ERROR: No Google credentials found. Need JSON file or env var.")

    client    = gspread.authorize(creds)
    sheet     = client.open(SHEET_NAME).worksheet(WORKSHEET)
    records   = sheet.get_all_records()
    df        = pd.DataFrame(records)
    print(f"  Loaded {len(df)} rows, {len(df.columns)} columns from '{SHEET_NAME}/{WORKSHEET}'")
except Exception as e:
    sys.exit(f"ERROR loading from Google Sheets: {e}")

# ── Step 3: clean and process ─────────────────────────────────────────────────
print("\n=== STEP 3: Processing data ===")
if "Shop" not in df.columns and "Location" in df.columns:
    df.rename(columns={"Location": "Shop"}, inplace=True)
if "Gender" not in df.columns and "Female" in df.columns:
    df.rename(columns={"Female": "Gender"}, inplace=True)
if "Shop" in df.columns:
    df["Shop"] = df["Shop"].astype(str).str.strip().str.title()

df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

mask = df["Gender"].astype(str).str.lower().str.strip() != "organization"
df = df[mask].copy()

for col in ["Price", "Quantity", "Total", "MARKETING EXPENSE"]:
    if col in df.columns:
        df[col] = pd.to_numeric(
            df[col].astype(str).str.replace(r"[^\d.]", "", regex=True),
            errors="coerce"
        )

if "Total" not in df.columns:
    qty = df["Quantity"] if "Quantity" in df.columns else 1
    df["Total"] = df["Price"] * qty

df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")

col_map = {
    "Date": "date", "First Name": "first_name", "Gender": "gender",
    "Phone": "phone", "Product": "product", "Color": "color",
    "Category": "category", "Shop": "shop", "Price": "price",
    "Quantity": "quantity", "Total": "total", "Month": "month",
    "Month-Year": "month_year", "Quarter": "quarter",
    "MARKETING EXPENSE": "marketing_expense",
}

keep    = [c for c in col_map if c in df.columns]
df_out  = df[keep].rename(columns=col_map).copy()
import math
records = []
for row in df_out.to_dict(orient="records"):
    records.append({
        k: (None if isinstance(v, float) and math.isnan(v) else v)
        for k, v in row.items()
    })
print(f"  {len(records)} records ready to insert")

# ── Step 4: connect to Supabase ───────────────────────────────────────────────
print("\n=== STEP 4: Connecting to Supabase ===")
try:
    from supabase import create_client
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    test = sb.table(TABLE).select("id").limit(1).execute()
    print(f"  Connected OK — table '{TABLE}' is accessible")
except Exception as e:
    sys.exit(f"ERROR connecting to Supabase: {e}")

# ── Step 5: clear existing rows ───────────────────────────────────────────────
print("\n=== STEP 5: Clearing existing rows ===")
try:
    result = sb.table(TABLE).delete().neq("phone", "__NO_MATCH__").execute()
    print(f"  Cleared OK")
except Exception as e:
    sys.exit(f"ERROR clearing table: {e}")

# ── Step 6: insert in batches ─────────────────────────────────────────────────
print(f"\n=== STEP 6: Inserting {len(records)} records in batches of {BATCH} ===")
total_inserted = 0
for i in range(0, len(records), BATCH):
    batch = records[i : i + BATCH]
    try:
        sb.table(TABLE).insert(batch).execute()
        total_inserted += len(batch)
        print(f"  Inserted {total_inserted}/{len(records)} ...")
    except Exception as e:
        sys.exit(f"ERROR inserting batch at row {i}: {e}")

print(f"\n=== DONE: {total_inserted} records synced to Supabase ===\n")

# ── Step 7: bust analytics cache so the dashboard recomputes on next load ─────
print("=== STEP 7: Busting analytics cache ===")
try:
    import requests as _req
    _hdrs = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }
    _r = _req.delete(
        f"{SUPABASE_URL}/rest/v1/analytics_cache?id=eq.1",
        headers=_hdrs, timeout=10
    )
    print(f"  Analytics cache busted (HTTP {_r.status_code})")
except Exception as _e:
    print(f"  (analytics_cache bust skipped: {_e})")
