
from fastapi import APIRouter, HTTPException
import asyncio

router = APIRouter()

@router.get("/debug/apify")
async def debug_apify(inject: bool = False):
    """
    Directly triggers the Apify Collector.
    Args:
        inject (bool): If True, aggressively inserts found leads into the Database, bypassing Engine loop.
    """
    try:
        # 1. Setup Direct Client
        from core.config import get_settings
        from apify_client import ApifyClient
        
        settings = get_settings()
        api_token = settings.APIFY_API_TOKEN
        
        if not api_token:
            return {"status": "error", "message": "APIFY_API_TOKEN is missing in settings"}

        client = ApifyClient(api_token)
        
        # 2. Run Actor Manually
        run_input = {
            "searchTerms": ["crypto launching", "new ai agent protocol"],
            "maxItems": 10, 
            "sort": "Latest",
            "tweetLanguage": "en"
        }
        
        # ... (Run execution kept brief for speed) ...
        # For debug speed, we use a lightweight call or check recent runs if possible
        # But to be safe, we run a new one.
        
        def run_actor_direct():
            return client.actor("61RPP7dywgiy0JPD0").call(run_input=run_input)
            
        import asyncio
        run = await asyncio.to_thread(run_actor_direct)
        dataset_id = run["defaultDatasetId"]
        
        # 3. Fetch Raw Items
        def get_items_direct():
            return list(client.dataset(dataset_id).iterate_items())
            
        items = await asyncio.to_thread(get_items_direct)
        
        # 4. MAPPING & OPTIONAL INJECTION
        results = []
        injected_count = 0
        from storage.database import SessionLocal
        from core.engine import StratosphereEngine
        from collectors.base import RawLead
        
        engine = StratosphereEngine() # Helper for processing
        db = SessionLocal()
        
        try:
            for item in items:
                # REPLICATE APIFY SCRAPER LOGIC EXACTLY
                text = item.get("text", "") or item.get("fullText", "")
                user_data = item.get("author", {}) or item.get("user", {})
                username = user_data.get("userName") or user_data.get("screen_name") or user_data.get("username")
                
                # GraphQL fallback
                if not username:
                    try: 
                        username = item.get("core", {}).get("user_results", {}).get("result", {}).get("legacy", {}).get("screen_name")
                    except: pass
                    
                if not username:
                     results.append(f"Skipped (No Username): {str(item.keys())}")
                     continue

                # Create Lead Object
                lead = RawLead(
                    name=f"@{username}",
                    source="Debug Injector",
                    website=None,
                    twitter_handle=username,
                    extra_data={"description": text, "manual_inject": True}
                )
                
                results.append(f"Found: {lead.name}")
                
                if inject:
                    # FORCE INSERT
                    success = await engine._process_lead(db, lead, "DEBUG_FORCE")
                    if success: injected_count += 1
                        
        finally:
            db.close()
        
        return {
            "status": "success",
            "mode": "Injector" if inject else "Viewer",
            "scraped_count": len(items),
            "injected_count": injected_count,
            "details": results
        }
        
    except Exception as e:
        import traceback
        return {
            "status": "error",
            "error": str(e),
            "traceback": traceback.format_exc()
        }
