 # =====================================================
# HELPER FUNCTION TO CREATE ALL TABLES
# =====================================================
def init_db():
    """
    Creates all tables including machines, production_logs,
    ERPNext metadata, and scheduled_jobs.
    Safe to call multiple times.
    """
    from models import Base  # ensure Base from models if needed
    Base.metadata.create_all(bind=engine)

# =====================================================
# database.py – Future-Proof Version for Taco Group HDPE
# Steps 1 → 42 + Step 43 (ScheduledJob Table)
# =====================================================
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime, timezone
import os

# =====================================================
# DATABASE CONFIG
# =====================================================
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./production.db")

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {},
    pool_pre_ping=True,
    future=True
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
    expire_on_commit=False
)

Base = declarative_base()

# =====================================================
# TABLE DEFINITIONS – FUTURE-PROOF
# =====================================================

class Machine(Base):
    __tablename__ = "machines"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer, primary_key=True, index=True)
    location = Column(String, index=True, nullable=False)
    name = Column(String, nullable=False)
    status = Column(String, default="free")
    work_order = Column(String, nullable=True)
    pipe_size = Column(String, nullable=True)
    target_qty = Column(Integer, default=0)
    produced_qty = Column(Integer, default=0)
    seconds_per_meter = Column(Float, default=0)
    last_tick_time = Column(DateTime, nullable=True)
    erpnext_work_order_id = Column(String, nullable=True)


class ProductionLog(Base):
    __tablename__ = "production_logs"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer, primary_key=True, index=True)
    machine_id = Column(Integer, nullable=False)
    location = Column(String, nullable=False)
    work_order = Column(String, nullable=True)
    pipe_size = Column(String, nullable=True)
    target_qty = Column(Integer, nullable=False, default=0)
    produced_qty = Column(Integer, default=0)
    remaining_qty = Column(Integer, default=0)
    status = Column(String, nullable=False, default="running")
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class ERPNextMetadata(Base):
    __tablename__ = "erpnext_metadata"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer, primary_key=True, index=True)
    machine_id = Column(Integer, nullable=False)
    work_order = Column(String, nullable=False)
    erp_status = Column(String, default="Not Started")
    erp_comments = Column(String, nullable=True)
    last_synced = Column(DateTime, default=lambda: datetime.now(timezone.utc))


# =====================================================
# STEP 43 → SCHEDULED JOBS TABLE
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
    priority = Column(Integer, default=0)  # Higher = urgent
    assigned_machine_id = Column(Integer, nullable=True)  # None = not assigned
    eta_seconds = Column(Float, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

# 🔥 PERMANENT AUTO SEED FUNCTION
def seed_default_machines():
    db = SessionLocal()
    try:
        # Check if any machine already exists
        existing = db.query(Machine).first()
        if existing:
            print("ℹ️ Machines already exist. Skipping seed.")
            return

        default_data = {
            "Modan": ["M1", "M2", "M3"],
            "Baldeya": ["B1", "B2"],
            "Al-Khraj": ["K1", "K2"]
        }

        for location, machines in default_data.items():
            for name in machines:
                db.add(Machine(
                    name=name,
                    location=location,
                    status="idle"
                ))

        db.commit()
        print("✅ Default machines seeded successfully.")

    except Exception as e:
        print("❌ Seed error:", e)
    finally:
        db.close()


# 🔥 AUTO RUN SEED ON IMPORT
seed_default_machines()