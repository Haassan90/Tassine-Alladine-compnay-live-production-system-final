# =====================================================
# fill_erpnext_missing_fields.py – Safe ERPNext Work Order Fix
# =====================================================

import os
import requests
from dotenv import load_dotenv

# =====================================================
# Load .env variables
# =====================================================
load_dotenv()

ERP_URL = os.getenv("ERP_URL")
API_KEY = os.getenv("ERP_API_KEY")
API_SECRET = os.getenv("ERP_API_SECRET")
TIMEOUT = int(os.getenv("ERP_TIMEOUT", 20))  # default 20s

HEADERS = {
    "Authorization": f"token {API_KEY}:{API_SECRET}",
    "Accept": "application/json",
    "Content-Type": "application/json"
}

# =====================================================
# Fetch all work orders
# =====================================================
def fetch_work_orders():
    url = f"{ERP_URL}/api/resource/Work Order"
    params = {
        "fields": '["name","custom_pipe_size","custom_location","custom_machine_id","status"]'
    }
    resp = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json().get("data", [])

# =====================================================
# Update a single work order
# =====================================================
def update_work_order(wo_name, updates: dict):
    url = f"{ERP_URL}/api/resource/Work Order/{wo_name}"
    resp = requests.put(url, json=updates, headers=HEADERS, timeout=TIMEOUT)
    resp.raise_for_status()
    print(f"✅ Updated {wo_name}: {updates}")

# =====================================================
# Main script – fill missing fields
# =====================================================
def fix_missing_fields():
    work_orders = fetch_work_orders()
    for wo in work_orders:
        updates = {}

        # Assign default location if missing
        if not wo.get("custom_location"):
            updates["custom_location"] = "Modan"  # <-- your real machine location

        # Assign default pipe_size if missing
        if not wo.get("custom_pipe_size"):
            updates["custom_pipe_size"] = "2\""  # <-- default size, adjust if needed

        # Optional: assign machine if not assigned
        if not wo.get("custom_machine_id"):
            updates["custom_machine_id"] = None  # Let auto-assign handle it

        # Push updates if needed
        if updates:
            update_work_order(wo["name"], updates)

if __name__ == "__main__":
    fix_missing_fields()
    print("✅ All missing fields fixed! Now restart backend for auto-assign to work.")
