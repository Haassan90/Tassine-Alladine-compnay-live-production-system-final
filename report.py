# =====================================================
# production_report.py
# Step 35 – Production Report Module (Updated & ERPNext Metadata)
# =====================================================
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from database import SessionLocal
from models import ProductionLog, Machine, ERPNextMetadata
from datetime import datetime
import csv
from io import StringIO
from fastapi.responses import StreamingResponse

router = APIRouter(prefix="/api/report", tags=["Production Report"])

# =====================================================
# DB Dependency
# =====================================================
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# =====================================================
# FETCH PRODUCTION LOGS
# =====================================================
@router.get("/logs")
def get_production_logs(
    start_date: str = Query(None, description="YYYY-MM-DD"),
    end_date: str = Query(None, description="YYYY-MM-DD"),
    location: str = Query(None, description="Filter by location"),
    db: Session = Depends(get_db)
):
    query = db.query(ProductionLog, Machine).join(Machine, Machine.id == ProductionLog.machine_id)

    # FILTER BY START DATE
    if start_date:
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            query = query.filter(ProductionLog.timestamp >= start_dt)
        except ValueError:
            pass

    # FILTER BY END DATE
    if end_date:
        try:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            query = query.filter(ProductionLog.timestamp <= end_dt)
        except ValueError:
            pass

    # FILTER BY LOCATION
    if location:
        query = query.filter(Machine.location == location)

    logs = query.order_by(ProductionLog.timestamp.desc()).all()

    result = []
    for log, machine in logs:
        # Fetch ERPNext metadata if available
        meta = db.query(ERPNextMetadata).filter(ERPNextMetadata.work_order == log.work_order).first()
        result.append({
            "machine_id": log.machine_id,
            "machine_name": machine.name,
            "location": machine.location,
            "work_order": log.work_order,
            "pipe_size": log.pipe_size,
            "produced_qty": log.produced_qty,
            "timestamp": log.timestamp.isoformat(),
            "erp_status": meta.erp_status if meta else None,
            "erp_comments": meta.erp_comments if meta else None
        })

    return {"logs": result}

# =====================================================
# CSV EXPORT
# =====================================================
@router.get("/export")
def export_production_csv(
    start_date: str = Query(None, description="YYYY-MM-DD"),
    end_date: str = Query(None, description="YYYY-MM-DD"),
    location: str = Query(None, description="Filter by location"),
    db: Session = Depends(get_db)
):
    data = get_production_logs(start_date, end_date, location, db).get("logs", [])

    if not data:
        return {"error": "No data found"}

    # CREATE CSV OUTPUT
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=list(data[0].keys()))
    writer.writeheader()
    for row in data:
        writer.writerow(row)
    output.seek(0)

    # FILENAME WITH TIMESTAMP
    filename = f"production_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )
