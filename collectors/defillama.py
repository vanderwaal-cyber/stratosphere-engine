import asyncio
import json
import time
from typing import List
from collectors.base import BaseCollector, RawLead

class DeFiLlamaCollector(BaseCollector):
    def __init__(self):
        super().__init__("defillama")
        self.api_url = "https://api.llama.fi"

    async def collect(self) -> List[RawLead]:
        leads = []
        try:
            self.logger.info("Fetching DeFiLlama Protocols...")
            # Fetch all protocols
            data = await self.fetch_page(f"{self.api_url}/protocols")
            protocols = json.loads(data)
            
            # Filter: Listed in last 90 days OR High TVL Growth
            # 90 days = 7776000 seconds
            now = time.time()
            recent_threshold = now - (90 * 24 * 3600)
            
            # Sort by listedAt (Newest first)
            # listedAt is unix timestamp (if available, mostly yes)
            def get_listed(p): return p.get('listedAt', 0) or 0
            
            new_protocols = sorted(
                [p for p in protocols if isinstance(p.get('listedAt'), (int, float))], 
                key=get_listed, 
                reverse=True
            )
            
            # Take a random slice of the top 300 to ensure variety on each run
            import random
            start_index = random.randint(0, 50)
            targets = new_protocols[start_index : start_index + 100]
            
            for p in targets:
                # DeFiLlama usually provides website and twitter!
                # This is HIGH QUALITY.
                website = p.get('url')
                twitter = p.get('twitter') # Just handle usually
                
                # Cleanup twitter handle
                if twitter and "twitter.com" in twitter:
                    twitter = twitter.split('/')[-1]
                
                if not website:
                    continue
                    
                leads.append(RawLead(
                    name=p.get('name'),
                    source="defillama_new",
                    website=website,
                    twitter_handle=twitter,
                    extra_data={
                        "tvl": p.get('tvl', 0),
                        "listed_at": p.get('listedAt'),
                        "chain": p.get('chain'),
                        "category": p.get('category')
                    }
                ))
                
        except Exception as e:
            self.logger.error(f"DeFiLlama failed: {e}")
            
        return leads
