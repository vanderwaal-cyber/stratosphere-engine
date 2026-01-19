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
        
        # 1. Fetch Trending (High Signal)
        try:
            self.logger.info("Fetching Trending Coins...")
            trending_json = await self.fetch_page(f"{self.api_url}/search/trending")
            trending_data = json.loads(trending_json)
            
            for item in trending_data.get('coins', []):
                coin = item['item']
                # Trending endpoint doesn't give links directly usually, need detail fetch?
                # Actually it gives 'data' sometimes.
                # If not, we have the ID.
                # Let's verify what we get. Usually min details.
                # We'll treat it as a lead and let enrichment find the site if missing.
                # But we really want the site.
                # Let's queue a detail fetch for these?
                # For speed, let's just get the name/symbol and let validation find the rest?
                # No, Requirements say: "Extract official website(s)".
                # Enrichment does that too.
                leads.append(RawLead(
                    name=coin['name'],
                    source="coingecko_trending",
                    extra_data={"symbol": coin['symbol'], "coingecko_id": coin['id']}
                ))
        except Exception as e:
            self.logger.error(f"Error fetching trending: {e}")

        # 2. Fetch Top Market Cap (Established)
        # We fetch pages 1 and 2 (Top 500)
        for page in [1, 2]:
            try:
                self.logger.info(f"Fetching Market Cap Page {page}...")
                market_json = await self.fetch_page(
                    f"{self.api_url}/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=250&page={page}&sparkline=false"
                )
                market_data = json.loads(market_json)
                
                for coin in market_data:
                    # Markets endpoint doesn't return Links. 
                    # We rely on Enrichment to find the site/twitter for established tokens.
                    # Or we call /coins/{id} for each? That hits rate limits FAST.
                    # Strategy: Return RawLead with just Name/Symbol. 
                    # The "Enrichment Engine" step 5 is "Fallback Search Resolver".
                    # This is efficient.
                    leads.append(RawLead(
                        name=coin['name'],
                        source="coingecko_market",
                        extra_data={"symbol": coin['symbol'], "market_cap": coin['market_cap']}
                    ))
                
                await asyncio.sleep(2) # Respect rate limit
            except Exception as e:
                self.logger.error(f"Error fetching page {page}: {e}")
                
        return leads
