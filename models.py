# =====================================================
# models.py – Taco Group Live Production Dashboard
# Steps 1 → 43 FULLY UPDATED & ERPNext Ready
# =====================================================

from sqlalchemy import Column, Integer, String, DateTime, Float, ForeignKey, Index
from database import Base
from datetime import datetime, timezone

# =====================================================
# MACHINE TABLE
# =====================================================
class Machine(Base):
    __tablename__ = "machines"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer, primary_key=True, index=True)
    location = Column(String, index=True, nullable=False)
    name = Column(String, nullable=False)
    status = Column(String, default="free", index=True)
    target_qty = Column(Integer, default=0)
    produced_qty = Column(Integer, default=0)
    seconds_per_meter = Column(Float, nullable=True)
    last_tick_time = Column(DateTime(timezone=True), nullable=True, default=lambda: datetime.now(timezone.utc))
    work_order = Column(String, nullable=True, default="")
    pipe_size = Column(String, nullable=True, default="")
    erpnext_work_order_id = Column(String, nullable=True, default="")

    # -------------------------------
    # HELPER METHODS
    # -------------------------------
    def is_running(self) -> bool:
        return self.status == "running"

    def is_completed(self) -> bool:
        return self.produced_qty >= self.target_qty > 0

    def remaining(self) -> int:
        return max(0, self.target_qty - self.produced_qty)


# =====================================================
# PRODUCTION HISTORY TABLE
# =====================================================
class ProductionHistory(Base):
    __tablename__ = "production_history"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer, primary_key=True, index=True)
    machine_id = Column(Integer, ForeignKey("machines.id"), nullable=False, index=True)
    location = Column(String, nullable=False)
    work_order = Column(String, nullable=True)
    pipe_size = Column(String, nullable=True)
    target_qty = Column(Integer, nullable=False, default=0)
    produced_qty = Column(Integer, nullable=False, default=0)
    remaining_qty = Column(Integer, nullable=False, default=0)
    status = Column(String, nullable=False)
    timestamp = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


# =====================================================
# PRODUCTION LOG TABLE
# =====================================================
class ProductionLog(Base):
    __tablename__ = "production_logs"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer, primary_key=True, index=True)
    machine_id = Column(Integer, ForeignKey("machines.id"), nullable=False, index=True)
    location = Column(String, nullable=False)
    work_order = Column(String, nullable=True)
    pipe_size = Column(String, nullable=True)
    target_qty = Column(Integer, nullable=False, default=0)
    produced_qty = Column(Integer, nullable=False, default=0)
    remaining_qty = Column(Integer, nullable=False, default=0)
    status = Column(String, nullable=False)
    timestamp = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


# =====================================================
# ERPNEXT METADATA
# =====================================================
class ERPNextMetadata(Base):
    __tablename__ = "erpnext_metadata"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer, primary_key=True, index=True)
    machine_id = Column(Integer, ForeignKey("machines.id"), nullable=False, index=True)
    work_order = Column(String, nullable=False, index=True)
    erp_status = Column(String, default="Not Started")
    erp_comments = Column(String, nullable=True)
    last_synced = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


# =====================================================
# SCHEDULED JOB TABLE
# =====================================================
class ScheduledJob(Base):
    __tablename__ = "scheduled_jobs"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer, primary_key=True, index=True)
    work_order = Column(String, nullable=False)
    location = Column(String, nullable=False)
    pipe_size = Column(String, nullable=True)
    qty = Column(Integer, default=0)
    produced_qty = Column(Integer, default=0)
    priority = Column(Integer, default=0)
    assigned_machine_id = Column(Integer, ForeignKey("machines.id"), nullable=True)
    eta_seconds = Column(Float, nullable=True)
    timestamp = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


# =====================================================
# INDEXING FOR PERFORMANCE
# =====================================================
Index("idx_machine_work_order", Machine.work_order)
Index("idx_erp_metadata_work_order", ERPNextMetadata.work_order)
Index("idx_production_log_location", ProductionLog.location)