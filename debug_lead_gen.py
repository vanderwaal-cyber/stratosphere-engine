import asyncio
import logging
import sys
import os

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("DEBUG_LEAD_GEN")

# Import Engine
try:
    from core.engine import StratosphereEngine
    from storage.database import SessionLocal
    from storage.models import Lead
except Exception as e:
    logger.error(f"Import Error: {e}")
    sys.exit(1)

async def test_generation():
    logger.info("Initializing Engine...")
    engine = StratosphereEngine()
    
    logger.info("Checking Database Connection...")
    db = SessionLocal()
    count = db.query(Lead).count()
    logger.info(f"Current Lead Count in DB: {count}")
    db.close()
    
    logger.info("Starting Test Run (Mode: Fresh)...")
    try:
        # We manually call run logic to see output
        await engine.run(mode="fresh", run_id="debug_test_01")
    except Exception as e:
        logger.error(f"Run Error: {e}")
        import traceback
        traceback.print_exc()

    logger.info("Run Logic Complete.")
    
    # Check Result
    db = SessionLocal()
    new_count = db.query(Lead).count()
    logger.info(f"New Lead Count in DB: {new_count} (Delta: {new_count - count})")
    
    # Check specifics
    latest = db.query(Lead).filter(Lead.run_id == "debug_test_01").all()
    logger.info(f"Leads found with ID 'debug_test_01': {len(latest)}")
    for l in latest[:5]:
        logger.info(f" Sample: {l.project_name} | {l.bucket} | {l.source_counts}")

if __name__ == "__main__":
    if os.path.exists("stratosphere.db"):
        logger.info(f"DB File Size: {os.path.getsize('stratosphere.db')} bytes")
    else:
        logger.warning("stratosphere.db NOT FOUND in current directory.")
        
    asyncio.run(test_generation())
