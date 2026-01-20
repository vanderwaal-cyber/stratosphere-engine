
from fastapi import APIRouter, HTTPException
from collectors.apify_scraper import ApifyXCollector
import asyncio

router = APIRouter()

@router.get("/debug/apify")
async def debug_apify():
    """
    Directly triggers the Apify Collector and returns the first 5 results.
    Bypasses Database and Deduplication.
    """
    collector = ApifyXCollector()
    
    try:
        # Pass a dummy callback
        leads = await collector.run(lambda **kwargs: None)
        
        return {
            "status": "success",
            "count": len(leads),
            "sample": [lead.__dict__ for lead in leads[:5]],
            "raw_log": "Check server logs for trace details."
        }
    except Exception as e:
        import traceback
        return {
            "status": "error",
            "error": str(e),
            "traceback": traceback.format_exc()
        }
