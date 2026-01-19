from core.config import get_settings
from storage.database import SessionLocal
from storage.models import Lead

try:
    s = get_settings()
    print(f"CMC_API_KEY in Settings: {'CMC_API_KEY' in s.model_dump()}")
    print(f"CMC_API_KEY Value: {s.CMC_API_KEY[:5]}...")
except Exception as e:
    print(f"Settings Error: {e}")

try:
    db = SessionLocal()
    count = db.query(Lead).count()
    print(f"Total Leads in DB: {count}")
    db.close()
except Exception as e:
    print(f"DB Error: {e}")
