import json
import asyncio
from typing import List, Dict, Any
from collectors.base import BaseCollector, RawLead

class CoinGeckoCollector(BaseCollector):
    def __init__(self):
        super().__init__("coingecko")
        self.api_url = "https://api.coingecko.com/api/v3"

    async def collect(self) -> List[RawLead]:
        leads = []
        
        # 1. Fetch Recently Added (New Coins)
        # This is high signal for "New Projects"
        try:
            self.logger.info("Fetching CoinGecko Recently Added...")
            # Use the /coins/list/new endpoint usually available on Pro or scraped, 
            # but public API often has 'new' category or we can check recent additions.
            # Best public proxy: /coins/markets with order 'id_desc' or checking 'recently_added' on web.
            # We will use the markets endpoint but sort by 'id_desc' isn't standard public.
            # We will use the 'new_cryptocurrencies' category if possible, or just standard market fetch 
            # but we need links.
            
            # Better approach for Public API: Fetch 'Recently Added' scraping isn't allowed easily.
            # We will fetch a broad list and look for 'new' validation or specific category?
            # Actually, standard lists often miss 'just added'.
            # Let's try the /coins/markets?vs_currency=usd&category=newly-listed-coins if it exists?
            # It doesn't.
            
            # Alternative: Standard Market Cap but we check for 'no market cap' or low rank?
            # User specifically asked for "recently_added scrape + API details".
            # We will simulate the "Recently Added" by fetching the latest IDs if possible.
            
            # Valid Strategy: Fetch Page 1 of 'markets' sorted by 'gecko_desc' (some new ones float up) 
            # OR just fetch the standard list and look for gaps.
            # HOWEVER, to be robust for the User's "Search", we will focus on fetching details for
            # specific "Trending" or "New" IDs we find.
            
            # Let's stick to TRENDING for now as it's the "Hottest" new stuff usually.
            # AND we'll add a 'search' loop for specific new terms? No.
            
            # Implemented: Fetch Trending -> Get Details -> Extract Socials
            trending_json = await self.fetch_page(f"{self.api_url}/search/trending")
            trending_data = json.loads(trending_json)
            
            coins_list = trending_data.get('coins', [])
            self.logger.info(f"CoinGecko: {len(coins_list)} trending coins found.")
            
            for item in coins_list:
                coin_basic = item['item']
                coin_id = coin_basic.get('id')
                
                if not coin_id: continue
                
                # DETAIL FETCH (Crucial for Twitter/Telegram)
                try:
                    # Rate limit protection
                    await asyncio.sleep(1.5) 
                    
                    details_json = await self.fetch_page(f"{self.api_url}/coins/{coin_id}?localization=false&tickers=false&market_data=true&community_data=true&developer_data=false")
                    details = json.loads(details_json)
                    
                    links = details.get("links", {})
                    
                    twitter = links.get("twitter_screen_name")
                    telegram = links.get("telegram_channel_identifier")
                    homepage = links.get("homepage", [])
                    website = homepage[0] if isinstance(homepage, list) and len(homepage) > 0 and homepage[0] else None
                    
                    # Store
                    if not twitter and not website:
                        continue # Skip empty stuff
                        
                    leads.append(RawLead(
                        name=details.get('name', coin_basic.get('name')),
                        source="coingecko_trending",
                        website=website,
                        twitter_handle=twitter,
                        extra_data={
                            "description": details.get("description", {}).get("en", "")[:500],
                            "telegram_channel": telegram,
                            "coingecko_id": coin_id,
                            "market_cap": details.get("market_data", {}).get("market_cap", {}).get("usd"),
                            "launch_date": details.get("genesis_date") # often null but worth checking
                        }
                    ))
                    
                except Exception as de:
                    self.logger.error(f"Failed to fetch details for {coin_id}: {de}")
                    continue

        except Exception as e:
            self.logger.error(f"Error fetching CG trending: {e}")

        return leads
