# =====================================================
# erpnext.py – FINAL PRODUCTION READY
# Full Project-Ready Version – Taco Group HDPE
# Includes ERP auto-update + safe dashboard sync
# =====================================================

import os
import requests
from typing import List, Dict
from database import SessionLocal
from models import Machine, ERPNextMetadata
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import asyncio
from dotenv import load_dotenv

# =====================================================
# LOAD ERP CREDENTIALS
# =====================================================
load_dotenv()
ERP_URL = os.getenv("ERP_URL")
API_KEY = os.getenv("ERP_API_KEY")
API_SECRET = os.getenv("ERP_API_SECRET")
TIMEOUT = 10  # seconds

HEADERS = {}
if API_KEY and API_SECRET:
    HEADERS = {
        "Authorization": f"token {API_KEY}:{API_SECRET}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

# =====================================================
# FETCH ACTIVE WORK ORDERS FROM ERPNext
# =====================================================
def get_work_orders() -> List[Dict]:
    """Fetch active Work Orders from ERPNext with auto-fix of missing fields."""
    if not ERP_URL or not HEADERS:
        print("⚠ ERP credentials missing")
        return []

    url = f"{ERP_URL}/api/resource/Work Order"
    params = {
        "fields": (
            '["name","qty","produced_qty","status",'
            '"custom_machine_id","custom_pipe_size","custom_location"]'
        ),
        "filters": (
            '[["status","in",["In Process","Not Started"]]]'
        )
    }

    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
        resp.raise_for_status()
        data = resp.json().get("data", []) or []

        # Auto-fix missing fields
        for wo in data:
            updates = {}
            if not wo.get("custom_location"):
                updates["custom_location"] = "Modan"
            if not wo.get("custom_pipe_size"):
                updates["custom_pipe_size"] = '2"'
            if updates:
                update_work_order_fields(wo["name"], updates)
                wo.update(updates)

        return data

    except requests.exceptions.Timeout:
        print("⏱ ERP request timeout")
    except requests.exceptions.RequestException as e:
        print("❌ ERP request failed:", e)
    except Exception as e:
        print("❌ ERP unknown error:", e)

    return []

# =====================================================
# UPDATE ERP WORK ORDER FIELDS (AUTO FIX)
# =====================================================
def update_work_order_fields(wo_name: str, updates: dict):
    if not wo_name or not updates or not ERP_URL:
        return
    try:
        url = f"{ERP_URL}/api/resource/Work Order/{wo_name}"
        requests.put(url, json=updates, headers=HEADERS, timeout=TIMEOUT).raise_for_status()
        print(f"✅ ERP WO {wo_name} fields updated: {updates}")
    except Exception as e:
        print(f"❌ ERP field update failed for {wo_name}: {e}")

# =====================================================
# UPDATE ERP WORK ORDER STATUS
# =====================================================
def update_work_order_status(wo_name: str, status: str):
    if not wo_name or not ERP_URL:
        return
    try:
        url = f"{ERP_URL}/api/resource/Work Order/{wo_name}"
        requests.put(url, json={"status": status}, headers=HEADERS, timeout=TIMEOUT).raise_for_status()
        print(f"🔄 ERP WO {wo_name} → {status}")
    except Exception as e:
        print(f"❌ ERP status update failed for {wo_name}: {e}")

# =====================================================
# SMART AUTO-ASSIGN WORK ORDERS TO MACHINES
# =====================================================
def auto_assign_work_orders(work_orders: List[Dict]):
    db = SessionLocal()
    try:
        for wo in work_orders:
            wo_name = wo.get("name")
            location = wo.get("custom_location")
            pipe_size = wo.get("custom_pipe_size")
            qty = wo.get("qty", 0)
            produced = wo.get("produced_qty", 0)

            # Skip already assigned in ERP
            if wo.get("custom_machine_id"):
                continue

            # Skip already assigned in DB
            if db.query(Machine).filter(Machine.erpnext_work_order_id == wo_name).first():
                continue

            free_machines = db.query(Machine).filter(
                Machine.location == location,
                Machine.status.in_(["free", "paused", "stopped"])
            ).all()

            assigned = False
            for m in free_machines:
                if not m.work_order and (m.pipe_size == pipe_size or not m.pipe_size):
                    m.work_order = wo_name
                    m.pipe_size = pipe_size
                    m.erpnext_work_order_id = wo_name
                    m.target_qty = qty
                    m.produced_qty = produced
                    m.status = "paused"

                    # ERP update
                    update_work_order_fields(wo_name, {"custom_machine_id": m.id})

                    # Metadata update
                    meta = db.query(ERPNextMetadata).filter(ERPNextMetadata.work_order == wo_name).first()
                    if not meta:
                        meta = ERPNextMetadata(machine_id=m.id, work_order=wo_name, erp_status="Assigned")
                        db.add(meta)
                    else:
                        meta.machine_id = m.id
                        meta.erp_status = "Assigned"

                    db.commit()
                    print(f"🟢 Assigned WO {wo_name} → Machine {m.name} ({location})")
                    assigned = True
                    break

            # Fallback: assign first free machine if no match
            if not assigned and free_machines:
                m = free_machines[0]
                m.work_order = wo_name
                m.pipe_size = pipe_size
                m.erpnext_work_order_id = wo_name
                m.target_qty = qty
                m.produced_qty = produced
                m.status = "paused"

                update_work_order_fields(wo_name, {"custom_machine_id": m.id})

                meta = db.query(ERPNextMetadata).filter(ERPNextMetadata.work_order == wo_name).first()
                if not meta:
                    meta = ERPNextMetadata(machine_id=m.id, work_order=wo_name, erp_status="Assigned")
                    db.add(meta)
                else:
                    meta.machine_id = m.id
                    meta.erp_status = "Assigned"

                db.commit()
                print(f"🟢 Assigned WO {wo_name} → Machine {m.name} ({location}) [fallback]")

    except SQLAlchemyError as e:
        db.rollback()
        print("❌ DB error during auto-assign:", e)
    finally:
        db.close()

# =====================================================
# ERPNext SYNC LOOP (ASYNC, BACKGROUND)
# =====================================================
async def erpnext_sync_loop(interval: int = 10):
    print("🚀 ERPNext Sync Loop started")
    while True:
        try:
            work_orders = get_work_orders()
            if work_orders:
                auto_assign_work_orders(work_orders)
        except Exception as e:
            print("❌ ERP Sync Loop error:", e)
        await asyncio.sleep(interval)
