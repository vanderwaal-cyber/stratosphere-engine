import sys
import os

# Ensure import paths work
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from storage.database import SessionLocal
from storage.models import Lead, RunLog

db = SessionLocal()
print("--- DB INSPECTION ---")
count = db.query(Lead).count()
print(f"Total Leads: {count}")

leads = db.query(Lead).limit(5).all()
for l in leads:
    print(f"Lead: {l.project_name} | Status: {l.status} | Bucket: {l.bucket} | Score: {l.score}")

print("\n--- RUN LOGS ---")
logs = db.query(RunLog).order_by(RunLog.timestamp.desc()).limit(5).all()
for log in logs:
    print(f"[{log.timestamp}] {log.level} - {log.message}")
