import asyncio
import logging
from core.engine import StratosphereEngine

# Configure logging
logging.basicConfig(level=logging.INFO)

async def main():
    engine = StratosphereEngine()
    print("Starting Engine Debug Scan...")
    try:
        await engine.run(mode="fresh", run_id="debug-run")
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
