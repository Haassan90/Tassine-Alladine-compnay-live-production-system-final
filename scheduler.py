# =====================================================
# scheduler.py – Production Scheduler + ERPNext Auto Sync
# =====================================================

import asyncio
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from database import SessionLocal
from models import Machine, ProductionHistory, ScheduledJob
from erpnext_sync import get_work_orders, auto_assign_work_orders  # Correct import
# from main import manager → circular import avoid, pass manager from main.py

SYNC_INTERVAL = 10           # seconds, ERPNext fetch interval
AUTO_ASSIGN_INTERVAL = 15    # seconds, auto-assign unassigned Work Orders
HISTORY_INTERVAL = 30        # seconds, snapshot history logging
SCHEDULED_JOB_INTERVAL = 10  # seconds, auto-assign ScheduledJobs

# =====================================================
# HELPER TO FORMAT DASHBOARD DATA
# =====================================================
def get_dashboard_data(db: Session):
    response = []
    machines = db.query(Machine).all()
    locations = {}
    
    # compute next job per location
    next_jobs = {}
    for m in machines:
        if m.status in ["free", "stopped"] and m.work_order:
            if m.location not in next_jobs:
                next_jobs[m.location] = {
                    "machine_id": m.id,
                    "work_order": m.work_order,
                    "pipe_size": m.pipe_size,
                    "total_qty": m.target_qty,
                    "produced_qty": m.produced_qty
                }

    for m in machines:
        remaining_qty = (m.target_qty - m.produced_qty) if m.target_qty else 0
        remaining_time = remaining_qty * m.seconds_per_meter if m.seconds_per_meter else None
        progress_percent = (m.produced_qty / m.target_qty * 100) if m.target_qty else 0

        locations.setdefault(m.location, []).append({
            "id": m.id,
            "name": m.name,
            "status": m.status,
            "job": {
                "work_order": m.work_order,
                "size": m.pipe_size,
                "total_qty": m.target_qty,
                "completed_qty": m.produced_qty,
                "remaining_qty": remaining_qty,
                "remaining_time": remaining_time,
                "progress_percent": progress_percent
            } if m.work_order else None,
            "next_job": next_jobs.get(m.location)
        })

    for loc, machines_list in locations.items():
        response.append({"name": loc, "machines": machines_list})

    return response

# =====================================================
# STEP 20 → ERPNext SYNC LOOP
# =====================================================
async def erpnext_sync_loop(manager):
    while True:
        db = SessionLocal()
        try:
            work_orders = get_work_orders()
            updated = False

            for wo in work_orders:
                machine_id = wo.get("custom_machine_id")
                location = wo.get("custom_location")
                if not machine_id or not location:
                    continue
                m = db.query(Machine).filter(
                    Machine.id == int(machine_id),
                    Machine.location == location
                ).first()
                if not m:
                    continue

                if m.work_order != wo.get("name") or m.pipe_size != wo.get("custom_pipe_size"):
                    m.work_order = wo.get("name")
                    m.pipe_size = wo.get("custom_pipe_size")
                    m.erpnext_work_order_id = wo.get("name")
                    updated = True

            if updated:
                db.commit()
                await manager.broadcast({"locations": get_dashboard_data(db)})

        except Exception as e:
            print(f"ERP SYNC ERROR: {e}")
        finally:
            db.close()

        await asyncio.sleep(SYNC_INTERVAL)

# =====================================================
# STEP 23 → AUTO-ASSIGN LOOP (ERPNext Work Orders)
# =====================================================
async def auto_assign_loop():
    while True:
        try:
            await asyncio.to_thread(auto_assign_work_orders)
        except Exception as e:
            print(f"Auto-assign loop error: {e}")
        await asyncio.sleep(AUTO_ASSIGN_INTERVAL)

# =====================================================
# STEP 24 → PRODUCTION HISTORY LOGGING
# =====================================================
async def production_history_loop():
    while True:
        db = SessionLocal()
        try:
            machines = db.query(Machine).all()
            timestamp = datetime.now(timezone.utc)
            for m in machines:
                remaining_qty = (m.target_qty - m.produced_qty) if m.target_qty else 0
                history = ProductionHistory(
                    machine_id=m.id,
                    location=m.location,
                    work_order=m.work_order,
                    pipe_size=m.pipe_size,
                    target_qty=m.target_qty,
                    produced_qty=m.produced_qty,
                    remaining_qty=remaining_qty,
                    status=m.status,
                    timestamp=timestamp
                )
                db.add(history)
            db.commit()
        except Exception as e:
            print(f"Production history loop error: {e}")
        finally:
            db.close()
        await asyncio.sleep(HISTORY_INTERVAL)

# =====================================================
# STEP 43 → SCHEDULED JOB AUTO-ASSIGN LOOP
# =====================================================
async def scheduled_job_auto_assign_loop(manager):
    while True:
        db = SessionLocal()
        try:
            jobs = db.query(ScheduledJob).filter(ScheduledJob.assigned_machine_id == None).all()
            free_machines = db.query(Machine).filter(Machine.status.in_(["free", "paused", "stopped"])).all()

            for job in jobs:
                location_machines = [m for m in free_machines if m.location == job.location]
                if not location_machines:
                    continue
                machine = location_machines[0]

                machine.work_order = job.work_order
                machine.pipe_size = job.pipe_size
                machine.target_qty = job.qty
                machine.produced_qty = job.produced_qty
                machine.status = "paused"
                machine.erpnext_work_order_id = job.work_order
                job.assigned_machine_id = machine.id

                db.commit()
                await manager.broadcast({
                    "locations": get_dashboard_data(db),
                    "scheduled_job_assigned": {"job_id": job.id, "machine_id": machine.id}
                })
        except Exception as e:
            print(f"Scheduled Job Auto-Assign Error: {e}")
        finally:
            db.close()
        await asyncio.sleep(SCHEDULED_JOB_INTERVAL)

# =====================================================
# STARTUP FUNCTION
# =====================================================
def start_scheduler(manager):
    """
    Call this from main.py startup_event
    Pass WebSocket manager as argument
    """
    asyncio.create_task(erpnext_sync_loop(manager))
    asyncio.create_task(auto_assign_loop())
    asyncio.create_task(production_history_loop())
    asyncio.create_task(scheduled_job_auto_assign_loop(manager))
