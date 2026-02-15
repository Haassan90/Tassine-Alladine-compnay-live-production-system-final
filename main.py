# =====================================================
# 🔒 main.py – Taco Group Live Production Dashboard (FINAL LOCKED)
# Updates: ERPNext safe sync + Scheduler + Auto Meter + Alerts + Admin ERP Orders
# =====================================================

import json
import os
import asyncio
import logging
from datetime import datetime, timezone
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from pydantic import BaseModel
from dotenv import load_dotenv

# =====================================================
# Load environment variables
# =====================================================
load_dotenv()
ERP_URL = os.getenv("ERP_URL")
ERP_API_KEY = os.getenv("ERP_API_KEY")
ERP_API_SECRET = os.getenv("ERP_API_SECRET")
DATABASE_URL = os.getenv("DATABASE_URL")

# =====================================================
# Import project modules
# =====================================================
from database import engine, SessionLocal, init_db
from models import Machine, ProductionLog, ERPNextMetadata
from erpnext_sync import (
    update_work_order_status, 
    get_work_orders, 
    auto_assign_work_orders, 
    get_admin_work_orders
)
from report import router as report_router
from scheduler import start_scheduler  # Scheduler with WebSocket manager

# =====================================================
# Logging
# =====================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("production_system.log"),
        logging.StreamHandler()
    ]
)

# =====================================================
# FastAPI App
# =====================================================
app = FastAPI(title="Taco Group Live Production")

ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "http://127.0.0.1:8000").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

app.include_router(report_router)

# =====================================================
# Database setup
# =====================================================
init_db()
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# =====================================================
# Frontend folder
# =====================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FRONTEND_DIR = os.path.join(BASE_DIR, "..", "Frontend")
app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")

@app.get("/")
async def get_dashboard():
    html_path = os.path.join(FRONTEND_DIR, "index.html")
    if not os.path.exists(html_path):
        return HTMLResponse("<h1>Dashboard HTML not found!</h1>", status_code=404)
    with open(html_path, "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

# =====================================================
# WebSocket Manager
# =====================================================
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active_connections.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active_connections:
            self.active_connections.remove(ws)

    async def broadcast(self, data: dict):
        dead_connections = []
        for ws in self.active_connections:
            try:
                await asyncio.wait_for(ws.send_json(data), timeout=2)
            except Exception:
                dead_connections.append(ws)

        for ws in dead_connections:
            self.disconnect(ws)

manager = ConnectionManager()

@app.websocket("/ws/dashboard")
async def ws_dashboard(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws)

# =====================================================
# Dashboard Helpers
# =====================================================
def get_dashboard_data(db: Session):
    response = []
    machines = db.query(Machine).all()
    metadata_map = {m.work_order: m for m in db.query(ERPNextMetadata).all()}
    locations = {}
    next_jobs = {}

    PIPE_SPEEDS = {"20mm": 20, "63mm": 40, "75mm": 55, "110mm": 85}

    for m in machines:
        remaining_qty = (m.target_qty - m.produced_qty) if m.target_qty else 0
        progress_percent = (m.produced_qty / m.target_qty) * 100 if m.target_qty else 0
        erp_meta = metadata_map.get(m.work_order)

        remaining_time = None
        if remaining_qty and m.pipe_size in PIPE_SPEEDS:
            remaining_time = remaining_qty * PIPE_SPEEDS[m.pipe_size]

        if m.status in ["free", "stopped"] and m.work_order:
            if m.location not in next_jobs:
                next_jobs[m.location] = {
                    "machine_id": m.id,
                    "work_order": m.work_order,
                    "pipe_size": m.pipe_size,
                    "total_qty": m.target_qty,
                    "produced_qty": m.produced_qty,
                    "remaining_time": remaining_time
                }

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
                "progress_percent": progress_percent,
                "erp_status": erp_meta.erp_status if erp_meta else None,
                "erp_comments": erp_meta.erp_comments if erp_meta else None
            } if m.work_order else None,
            "next_job": next_jobs.get(m.location)
        })

    for loc, machines_list in locations.items():
        response.append({"name": loc, "machines": machines_list})

    return response

# =====================================================
# API Endpoints
# =====================================================
@app.get("/api/dashboard")
def dashboard(db: Session = Depends(get_db)):
    return {"locations": get_dashboard_data(db)}

@app.get("/api/job_queue")
def job_queue():
    try:
        work_orders = get_work_orders()
    except Exception:
        work_orders = []
    queue = [({
        "id": wo.get("name"),
        "pipe_size": wo.get("custom_pipe_size"),
        "qty": wo.get("qty"),
        "produced_qty": wo.get("produced_qty", 0),
        "location": wo.get("custom_location"),
        "machine_id": wo.get("custom_machine_id")
    }) for wo in work_orders if wo.get("status") != "Completed"]
    return {"queue": queue}

# =====================================================
# Assign Work Order Model
# =====================================================
class AssignWorkOrder(BaseModel):
    work_order: str
    location: str
    machine_id: str
    pipe_size: str
    target_qty: int
    role: str  # operator / manager

# =====================================================
# API – Assign Work Order (Drag & Drop)
# =====================================================
@app.post("/api/machine/assign")
async def assign_work_order(data: AssignWorkOrder, db: Session = Depends(get_db)):
    machine = get_machine(db, data.location, data.machine_id)

    if not machine:
        return {"ok": False, "error": "Machine not found"}

    if data.role not in ["operator", "manager"]:
        return {"ok": False, "error": "Unauthorized role"}

    if machine.status == "running":
        queue = json.loads(machine.next_work_orders or "[]")
        queue.append({
            "work_order": data.work_order,
            "pipe_size": data.pipe_size,
            "target_qty": data.target_qty
        })
        machine.next_work_orders = json.dumps(queue)
        db.commit()
        await manager.broadcast({
            "message": f"Work order {data.work_order} queued on {machine.name}",
            "machine_id": machine.id
        })
        return {"ok": True, "queued": True}

    machine.work_order = data.work_order
    machine.pipe_size = data.pipe_size
    machine.target_qty = data.target_qty
    machine.produced_qty = 0
    machine.status = "stopped"
    machine.erpnext_work_order_id = data.work_order
    machine.next_work_orders = "[]"
    db.commit()

    try:
        if ERP_URL and ERP_API_KEY and ERP_API_SECRET:
            update_work_order_status(data.work_order, "In Process")
    except Exception as e:
        logging.error(f"ERP machine update failed: {e}")

    await manager.broadcast({
        "message": f"Work order {data.work_order} assigned to {machine.name}",
        "machine_id": machine.id
    })

    return {"ok": True, "assigned": True}

# =====================================================
# Admin ERP Orders
# =====================================================
@app.get("/api/admin/work_orders")
def admin_work_orders():
    try:
        work_orders = get_admin_work_orders()
    except Exception:
        work_orders = []
    return {"work_orders": [{
        "id": wo.get("name"),
        "status": wo.get("status"),
        "pipe_size": wo.get("custom_pipe_size"),
        "qty": wo.get("qty"),
        "produced_qty": wo.get("produced_qty", 0),
        "location": wo.get("custom_location"),
        "machine_id": wo.get("custom_machine_id")
    } for wo in work_orders]}

# =====================================================
# Machine Controls Models
# =====================================================
class MachineAction(BaseModel):
    location: str
    machine_id: str

class MachineRename(MachineAction):
    new_name: str

# =====================================================
# Helper – Update Machine Status
# =====================================================
async def update_machine_status(db: Session, m: Machine, new_status: str):
    m.status = new_status
    try:
        if new_status == "running":
            m.is_locked = True
            m.last_tick_time = datetime.now(timezone.utc)
            if ERP_URL and ERP_API_KEY and ERP_API_SECRET and m.erpnext_work_order_id:
                update_work_order_status(m.erpnext_work_order_id, "In Process")
        elif new_status == "completed":
            m.is_locked = False
            if ERP_URL and ERP_API_KEY and ERP_API_SECRET and m.erpnext_work_order_id:
                update_work_order_status(m.erpnext_work_order_id, "Completed")
    except Exception as e:
        logging.error(f"ERPNext status update failed: {e}")
    db.commit()

# =====================================================
# Helper – Get Machine
# =====================================================
def get_machine(db: Session, location: str, machine_id: str) -> Machine | None:
    try:
        machine_id = int(machine_id)
    except:
        return None
    return db.query(Machine).filter(
        Machine.id == machine_id,
        Machine.location == location
    ).first()

# =====================================================
# API – Machine Controls
# =====================================================
@app.post("/api/machine/start")
async def start_machine(data: MachineAction, db: Session = Depends(get_db)):
    m = get_machine(db, data.location, data.machine_id)
    if not m or not m.work_order:
        return {"ok": False, "error": "Machine not found or no active work order"}
    await update_machine_status(db, m, "running")
    return {"ok": True, "machine": {"id": m.id, "status": m.status}}

@app.post("/api/machine/pause")
async def pause_machine(data: MachineAction, db: Session = Depends(get_db)):
    m = get_machine(db, data.location, data.machine_id)
    if not m:
        return {"ok": False, "error": "Machine not found"}
    m.status = "paused"
    db.commit()
    return {"ok": True, "machine": {"id": m.id, "status": m.status}}

@app.post("/api/machine/stop")
async def stop_machine(data: MachineAction, db: Session = Depends(get_db)):
    m = get_machine(db, data.location, data.machine_id)
    if not m:
        return {"ok": False, "error": "Machine not found"}
    await update_machine_status(db, m, "stopped")
    return {"ok": True, "machine": {"id": m.id, "status": m.status}}

@app.post("/api/machine/rename")
async def rename_machine(data: MachineRename, db: Session = Depends(get_db)):
    m = get_machine(db, data.location, data.machine_id)
    if not m:
        return {"ok": False, "error": "Machine not found"}
    old_name = m.name
    m.name = data.new_name
    db.commit()
    return {
        "ok": True,
        "machine": {"id": m.id, "old_name": old_name, "new_name": m.name}
    }

# =====================================================
# Automatic Meter Counter
# =====================================================
async def automatic_meter_counter():
    PIPE_SPEEDS = {"20mm": 20, "63mm": 40, "75mm": 55, "110mm": 85}
    while True:
        await asyncio.sleep(1)
        db = SessionLocal()
        try:
            machines = db.query(Machine).filter(Machine.status == "running").all()
            now = datetime.now(timezone.utc)
            for m in machines:
                if not m.work_order or not m.pipe_size:
                    continue
                seconds_per_meter = PIPE_SPEEDS.get(m.pipe_size)
                if not seconds_per_meter:
                    logging.warning(f"No speed defined for size {m.pipe_size}")
                    continue
                last_tick = m.last_tick_time or now
                if last_tick.tzinfo is None:
                    last_tick = last_tick.replace(tzinfo=timezone.utc)
                diff = (now - last_tick).total_seconds()
                ticks = int(diff // seconds_per_meter)
                if ticks > 0 and m.produced_qty < m.target_qty:
                    increment = min(ticks, m.target_qty - m.produced_qty)
                    m.produced_qty += increment
                    m.last_tick_time = now
                    remaining_qty = m.target_qty - m.produced_qty
                    db.add(ProductionLog(
                        machine_id=m.id,
                        location=m.location or "Unknown",
                        work_order=m.work_order,
                        pipe_size=m.pipe_size,
                        produced_qty=increment,
                        remaining_qty=remaining_qty,
                        status="running",
                        timestamp=now
                    ))
                    meta = db.query(ERPNextMetadata).filter(
                        ERPNextMetadata.work_order == m.work_order
                    ).first()
                    if meta:
                        meta.erp_status = "In Progress"
                        meta.last_synced = now
                    if m.produced_qty >= m.target_qty:
                        m.produced_qty = m.target_qty
                        logging.info(f"Machine {m.id} completed {m.work_order}")
                        queue = json.loads(m.next_work_orders or "[]")
                        if queue:
                            next_job = queue.pop(0)
                            m.work_order = next_job["work_order"]
                            m.pipe_size = next_job["pipe_size"]
                            m.target_qty = next_job["target_qty"]
                            m.produced_qty = 0
                            m.next_work_orders = json.dumps(queue)
                            m.last_tick_time = now
                            m.status = "running"
                            logging.info(f"Machine {m.id} auto-loaded next job {m.work_order}")
                        else:
                            m.status = "completed"
                        if meta:
                            meta.erp_status = "Completed"
                            meta.last_synced = now
            db.commit()
        except Exception as e:
            logging.error(f"AUTO METER ERROR: {e}")
            db.rollback()
        finally:
            db.close()

# =====================================================
# Production Alerts
# =====================================================
alert_history = {}
async def production_alerts():
    while True:
        await asyncio.sleep(1)
        db = SessionLocal()
        try:
            machines = db.query(Machine).filter(Machine.target_qty > 0).all()
            for m in machines:
                if not m.work_order or m.status != "running":
                    continue
                percent = (m.produced_qty / m.target_qty) * 100 if m.target_qty else 0
                last_level = alert_history.get(m.id, 0)
                alert_level = 0
                message = None
                if percent >= 100:
                    alert_level = 3
                    message = f"✅ Machine {m.name} COMPLETED"
                elif percent >= 90:
                    alert_level = 2
                    message = f"⚠ {m.name} CRITICAL {percent:.1f}%"
                elif percent >= 75:
                    alert_level = 1
                    message = f"⚠ {m.name} Warning {percent:.1f}%"
                if alert_level > 0 and alert_level != last_level:
                    alert_history[m.id] = alert_level
                    await manager.broadcast({"alert": message, "machine_id": m.id, "level": alert_level})
                elif percent < 75:
                    alert_history[m.id] = 0
        except Exception as e:
            logging.error(f"ALERT LOOP ERROR: {e}")
        finally:
            db.close()
        await asyncio.sleep(5)

# =====================================================
# ERPNext Sync Loop
# =====================================================
async def erpnext_sync_loop(interval: int = 10):
    logging.info("🚀 ERPNext Sync Loop started")
    while True:
        await asyncio.sleep(1)
        try:
            await asyncio.to_thread(auto_assign_work_orders)
        except Exception as e:
            logging.error(f"ERP Sync Loop error: {e}")
        await asyncio.sleep(interval)

# =====================================================
# Broadcast Dashboard + ERP Queue
# =====================================================
async def broadcast_dashboard_and_erpnext():
    while True:
        await asyncio.sleep(1)
        db = SessionLocal()
        try:
            locations = get_dashboard_data(db)
            try:
                work_orders = get_work_orders()
            except Exception:
                work_orders = []
            erp_queue = [{
                "id": wo.get("name"),
                "status": wo.get("status"),
                "pipe_size": wo.get("custom_pipe_size"),
                "qty": wo.get("qty"),
                "produced_qty": wo.get("produced_qty", 0),
                "location": wo.get("custom_location"),
                "machine_id": wo.get("custom_machine_id")
            } for wo in work_orders]
            await manager.broadcast({
                "locations": locations,
                "work_orders": erp_queue
            })
        except Exception as e:
            logging.error(f"BROADCAST ERROR: {e}")
        finally:
            db.close()
        await asyncio.sleep(5)

# =====================================================
# Startup Event
# =====================================================
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(automatic_meter_counter(), name="AutomaticMeterCounter")
    asyncio.create_task(production_alerts(), name="ProductionAlerts")
    asyncio.create_task(erpnext_sync_loop(), name="ERPNextSyncLoop")
    asyncio.create_task(broadcast_dashboard_and_erpnext(), name="BroadcastDashboard")
    start_scheduler(manager)

# =====================================================
# Production Logs API
# =====================================================
@app.get("/api/production_logs")
def production_logs(db: Session = Depends(get_db)):
    logs = db.query(ProductionLog).order_by(
        ProductionLog.timestamp.desc()
    ).limit(200).all()
    return [
        {
            "id": log.id,
            "machine_id": log.machine_id,
            "work_order": log.work_order,
            "pipe_size": log.pipe_size,
            "produced_qty": log.produced_qty,
            "timestamp": log.timestamp.isoformat() if log.timestamp else None
        }
        for log in logs
    ]
