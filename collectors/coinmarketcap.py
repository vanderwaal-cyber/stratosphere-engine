import json
import asyncio
from typing import List, Dict, Any
from collectors.base import BaseCollector, RawLead
import httpx

class CoinMarketCapCollector(BaseCollector):
    def __init__(self):
        super().__init__("coinmarketcap")
        self.api_key = self.settings.CMC_API_KEY
        self.base_url = "https://pro-api.coinmarketcap.com"

    def get_headers(self):
        return {
            'X-CMC_PRO_API_KEY': self.api_key,
            'Accept': 'application/json'
        }

    async def collect(self) -> List[RawLead]:
        leads = []
        if not self.api_key:
            self.logger.warning("CMC_API_KEY not found. Skipping CoinMarketCap.")
            return []

        try:
            # 1. Fetch Latest Listings (Limit 200)
            self.logger.info("Fetching CMC Latest Listings...")
            
            # 1. Fetch Latest Listings (Limit 5000)
            self.logger.info("Fetching CMC Latest Listings...")
            
            # Increase timeout for heavy data load
            async with httpx.AsyncClient(timeout=60.0) as client:
                # Get latest added
                resp = await client.get(
                    f"{self.base_url}/v1/cryptocurrency/listings/latest",
                    headers=self.get_headers(),
                    params={
                        "start": "1",
                        "limit": "5000",
                        "sort": "date_added",
                        "sort_dir": "desc",
                        "aux": "date_added,tags,platform"
                    }
                )
                resp.raise_for_status()
                data = resp.json().get("data", [])
                
                # Extract IDs for batch info fetch
                ids = [str(coin["id"]) for coin in data]
                
                coin_details = {}
                
                chunk_size = 100
                for i in range(0, len(ids), chunk_size):
                    chunk_ids = ids[i:i + chunk_size]
                    ids_str = ",".join(chunk_ids)
                    
                    self.logger.info(f"Fetching properties for chunk {i}...")
                    try:
                        info_resp = await client.get(
                            f"{self.base_url}/v2/cryptocurrency/info",
                            headers=self.get_headers(),
                            params={"id": ids_str}
                        )
                        info_resp.raise_for_status()
                        batch_data = info_resp.json().get("data", {})
                        coin_details.update(batch_data)
                    except Exception as e:
                        self.logger.error(f"Failed to fetch batch info: {e}")
                
                # Process leads
                for coin in data:
                    coin_id = str(coin["id"])
                    details = coin_details.get(coin_id, {})
                    urls = details.get("urls", {})
                    
                    # Extract Socials
                    twitter = None
                    if urls.get("twitter") and isinstance(urls["twitter"], list) and len(urls["twitter"]) > 0:
                        twitter = urls["twitter"][0]
                    
                    telegram = None
                    if urls.get("chat") and isinstance(urls["chat"], list):
                        for chat in urls["chat"]:
                            if "t.me" in chat or "telegram" in chat:
                                telegram = chat
                                break
                    
                    website = None
                    if urls.get("website") and isinstance(urls["website"], list) and len(urls["website"]) > 0:
                        website = urls["website"][0]

                    # Extract Logo (Profile Picture)
                    logo = details.get("logo")
                        
                    # Extract Tags
                    tags = coin.get("tags", [])
                    
                    # Chains
                    platform = coin.get("platform")
                    chain_name = platform.get("name") if platform else None
                    
                    lead = RawLead(
                        name=coin["name"],
                        source="coinmarketcap",
                        website=website,
                        twitter_handle=twitter,
                        profile_image_url=logo, # Explicitly pass the CMC logo
                        extra_data={
                            "symbol": coin["symbol"],
                            "description": details.get("description"),
                            "tags": tags,
                            "chains": [chain_name] if chain_name else [],
                            "launch_date": coin.get("date_added"),
                            "telegram_channel": telegram,
                            "cmc_id": coin_id
                        }
                    )
                    leads.append(lead)
                        
        except Exception as e:
            self.logger.error(f"CMC Fetch Error: {e}", exc_info=True)
            
        return leads
