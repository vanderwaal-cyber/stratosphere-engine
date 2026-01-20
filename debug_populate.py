from storage.database import SessionLocal
from storage.models import Lead
from datetime import datetime

db = SessionLocal()

print("Checking existing leads...")
count = db.query(Lead).count()
print(f"Current Lead Count: {count}")

if count == 0:
    print("Inserting Dummy Lead...")
    dummy = Lead(
        project_name="Debug Token",
        source="debug_script",
        domain="https://debugtoken.com",
        twitter_handle="https://x.com/debugtoken",
        description="A test token manually inserted.",
        status="New",
        bucket="READY_TO_DM",
        score=100,
        telegram_channel="https://t.me/debugtoken",
        launch_date=datetime.utcnow(),
        chains="['Solana']",
        tags="['Debug', 'AI']"
    )
    db.add(dummy)
    db.commit()
    print("Dummy Lead Inserted.")

print("Verifying API Response structure...")
import requests
try:
    # Assuming running locally
    r = requests.get("http://localhost:8000/api/leads")
    if r.status_code == 200:
        data = r.json()
        print(f"API Returned {len(data)} leads.")
        if len(data) > 0:
            print(f"Sample Lead: {data[0]['project_name']}")
    else:
        print(f"API Error {r.status_code}: {r.text}")
except Exception as e:
    print(f"Request Failed: {e}")

db.close()
