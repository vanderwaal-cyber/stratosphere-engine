
from fastapi import APIRouter, HTTPException
@router.get("/debug/apify")
async def debug_apify():
    """
    Directly triggers the Apify Collector and returns the first 5 results.
    Bypasses Database and Deduplication.
    """
    # Lazy Import to prevent Circular Import crashing Main
    from collectors.apify_scraper import ApifyXCollector
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
