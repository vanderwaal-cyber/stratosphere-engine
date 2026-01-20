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
            
            # Filter: Listed in last 7 days AND TVL > $10k
            # 7 days = 604800 seconds
            now = time.time()
            recent_threshold = now - (7 * 24 * 3600)
            min_tvl = 10000
            
            # Sort by listedAt (Newest first)
            def get_listed(p): return p.get('listedAt', 0) or 0
            
            # strict filter
            new_protocols = sorted(
                [p for p in protocols if isinstance(p.get('listedAt'), (int, float)) and p.get('listedAt') > recent_threshold and p.get('tvl', 0) >= min_tvl], 
                key=get_listed, 
                reverse=True
            )
            
            # Take all valid new ones (usually small number for 7 days)
            targets = new_protocols
            
            self.logger.info(f"DeFiLlama: Found {len(targets)} protocols listed in last 7d with TVL > $10k")

            for p in targets:
                # DeFiLlama usually provides website and twitter!
                website = p.get('url')
                twitter = p.get('twitter') 
                
                # STRICT REQUIREMENT: Must have Twitter for outreach
                if not twitter: continue

                # Cleanup twitter handle
                if "twitter.com" in twitter or "x.com" in twitter:
                    twitter = twitter.split('/')[-1]
                
                if "?" in twitter: twitter = twitter.split("?")[0]
                
                if not website:
                    continue
                    
                # Normalize Chains
                chains = p.get('chains', [])
                if not chains and p.get('chain'):
                    chains = [p.get('chain')]
                
                # Launch Date
                launch_date = None
                if p.get('listedAt'):
                    import datetime
                    try:
                        launch_date = datetime.datetime.fromtimestamp(p.get('listedAt'))
                    except: pass

                leads.append(RawLead(
                    name=p.get('name'),
                    source="defillama_new",
                    website=website,
                    twitter_handle=twitter,
                    extra_data={
                        "tvl": p.get('tvl', 0),
                        "launch_date": launch_date,
                        "chains": chains,
                        "tags": [p.get('category')] if p.get('category') else [],
                        "description": p.get('description'),
                        "defillama_id": p.get('id')
                    }
                ))
                
        except Exception as e:
            self.logger.error(f"DeFiLlama failed: {e}")
            
        return leads
