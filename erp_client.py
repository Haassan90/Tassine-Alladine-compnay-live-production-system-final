# =====================================================
# erpclient.py – ERPNext REST Client (Production Ready)
# =====================================================

import os
import requests
from dotenv import load_dotenv

# =====================================================
# Load .env variables
# =====================================================
load_dotenv()

ERP_URL = os.getenv("ERP_URL")  # e.g. http://127.0.0.1:8000
ERP_API_KEY = os.getenv("API_KEY")
ERP_API_SECRET = os.getenv("API_SECRET")
ERP_TIMEOUT = int(os.getenv("ERP_TIMEOUT", 20))

if not ERP_URL:
    raise ValueError("ERP_URL missing in .env")

HEADERS = {}
if ERP_API_KEY and ERP_API_SECRET:
    HEADERS = {
        "Authorization": f"token {ERP_API_KEY}:{ERP_API_SECRET}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }


# =====================================================
# Fetch Work Orders from ERPNext
# =====================================================
def fetch_work_orders(status=None):
    """
    Fetch Work Orders from ERPNext
    Optional filter by status
    """
    try:
        url = f"{ERP_URL}/api/resource/Work Order"

        params = {
            "fields": '["name","production_item","qty","status","custom_pipe_size","custom_location","custom_machine_id"]'
        }

        if status:
            params["filters"] = f'[["status","=","{status}"]]'

        response = requests.get(
            url,
            headers=HEADERS,
            params=params,
            timeout=ERP_TIMEOUT
        )

        response.raise_for_status()
        return response.json().get("data", [])

    except requests.RequestException as e:
        print(f"❌ ERP Fetch Error: {e}")
        return []


# =====================================================
# Update Work Order in ERPNext
# =====================================================
def update_work_order(work_order_name, updates: dict):
    """
    Update any field in Work Order
    Example:
    update_work_order("WO-0001", {"status": "In Process"})
    """
    try:
        url = f"{ERP_URL}/api/resource/Work Order/{work_order_name}"

        response = requests.put(
            url,
            json=updates,
            headers=HEADERS,
            timeout=ERP_TIMEOUT
        )

        response.raise_for_status()
        print(f"✅ ERP Updated: {work_order_name}")
        return response.json()

    except requests.RequestException as e:
        print(f"❌ ERP Update Error: {e}")
        return None


# =====================================================
# Assign Machine to Work Order
# =====================================================
def assign_machine(work_order_name, machine_id):
    """
    Assign machine to Work Order
    """
    return update_work_order(
        work_order_name,
        {"custom_machine_id": machine_id}
    )


# =====================================================
# Mark Work Order Completed
# =====================================================
def mark_completed(work_order_name):
    """
    Change status to Completed
    """
    return update_work_order(
        work_order_name,
        {"status": "Completed"}
    )
