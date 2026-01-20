
from fastapi import APIRouter, HTTPException
import asyncio

router = APIRouter()

@router.get("/debug/apify")
async def debug_apify():
    """
    Directly triggers the Apify Collector and returns the RAW Apify Dataset items.
    Bypasses Database, Deduplication, and internal Class Mapping.
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
        # Use a very specific query to ensure non-empty results if working
        run_input = {
            "searchTerms": ["crypto launching has:links"],
            "maxItems": 5, 
            "sort": "Latest",
            "tweetLanguage": "en"
        }
        
        print("DEBUG: Sending request to Apify...")
        
        def run_actor_direct():
            # Actor: apidojo/tweet-scraper
            return client.actor("61RPP7dywgiy0JPD0").call(run_input=run_input)
            
        run = await asyncio.to_thread(run_actor_direct)
        dataset_id = run["defaultDatasetId"]
        
        print(f"DEBUG: Dataset ID: {dataset_id}")
        
        # 3. Fetch Raw Items
        def get_items_direct():
            return list(client.dataset(dataset_id).iterate_items())
            
        items = await asyncio.to_thread(get_items_direct)
        
        # 4. TEST MAPPING LOGIC (Simulate Collector)
        mapped_leads = []
        mapping_errors = []
        
        for item in items:
            try:
                # COPY OF LOGIC FROM apify_scraper.py
                text = item.get("text", "") or item.get("fullText", "")
                user_data = item.get("author", {}) or item.get("user", {})
                username = user_data.get("userName") or user_data.get("screen_name") or user_data.get("username")
                
                if not username:
                    mapping_errors.append(f"Missing username. Keys: {user_data.keys()}")
                    continue
                    
                mapped_leads.append(f"@{username}")
            except Exception as e:
                mapping_errors.append(str(e))
        
        return {
            "status": "success",
            "count": len(items),
            "mapped_count": len(mapped_leads),
            "mapped_samples": mapped_leads[:5],
            "mapping_errors": mapping_errors[:3],
            "raw_sample": items[:1] 
        }
