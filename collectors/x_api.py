import asyncio
import httpx
from typing import List, Optional
from collectors.base import BaseCollector, RawLead

class XApiCollector(BaseCollector):
    def __init__(self):
        super().__init__("x_api")
        self.bearer_token = self.settings.X_BEARER_TOKEN
        self.base_url = "https://api.twitter.com/2"

    async def collect(self) -> List[RawLead]:
        leads = []
        if not self.bearer_token:
            self.logger.warning("X_BEARER_TOKEN not found. Skipping X/Twitter collection.")
            return []

        # "X Fresh Scanner" - Broad Mode (No strict Telegram filter)
        # We eagerly look for launch signals with ANY link.
        queries = [
            # 1. LAUNCH & NEW PROTOCOLS (Broad)
            '(("launching" OR "IDO" OR "TGE" OR "testnet" OR "DePIN" OR "AI agent" OR "NFT drop" OR "new protocol") has:links -is:retweet min_faves:2)',
            
            # 2. CONTRACTS / CA (Degen)
            '(("contract address" OR "ca:") (solana OR eth OR base) has:links -is:retweet min_faves:2)',
            
            # 3. NARRATIVES
            '(("DePIN" OR "AI Agent") ("roadmap" OR "whitepaper" OR "building") has:links -is:retweet min_faves:2)'
        ]

        async with httpx.AsyncClient(headers={"Authorization": f"Bearer {self.bearer_token}"}, timeout=45) as client:
            for query in queries:
                next_token = None
                pages_fetched = 0
                
                # RECURSIVE PAGINATION LOOP
                while True:
                    if len(leads) >= 500: break # Hard stop total
                    if pages_fetched > 5: break # Max depth per query
                    
                    try:
                        self.logger.info(f"🔎 X Fresh Scan (Page {pages_fetched+1}): {query[:40]}...")
                        
                        params = {
                            "query": query,
                            "max_results": 100,
                            "tweet.fields": "created_at,author_id,entities,public_metrics,text",
                            "expansions": "author_id",
                            "user.fields": "username,description,url,entities,public_metrics"
                        }
                        if next_token:
                            params["next_token"] = next_token
                            
                        resp = await client.get(
                            f"{self.base_url}/tweets/search/recent",
                            params=params
                        )
                        
                        if resp.status_code == 429:
                            self.logger.warning("X API Rate Limit hit. Cooling down...")
                            await asyncio.sleep(5) 
                            break 
                            
                        if resp.status_code != 200:
                            self.logger.error(f"X API Error {resp.status_code}: {resp.text}")
                            break
                            
                        data = resp.json()
                        tweets = data.get("data", [])
                        users = {u["id"]: u for u in data.get("includes", {}).get("users", [])}
                        meta = data.get("meta", {})
                        
                        self.logger.info(f"   -> Found {len(tweets)} candidates on page {pages_fetched+1}.")
                        
                        # Process Tweets
                        for tweet in tweets:
                            author_id = tweet.get("author_id")
                            user = users.get(author_id, {})
                            username = user.get("username")
                            
                            if not username: continue
                            
                            metrics = tweet.get("public_metrics", {})
                            
                            # Extract Links
                            website = None
                            telegram = None
                            
                            # PRIORITY 1: User Profile
                            if "entities" in user and "url" in user["entities"]:
                                for url in user["entities"]["url"].get("urls", []):
                                    expanded = url.get("expanded_url", "")
                                    if "t.me" in expanded or "telegram.me" in expanded: telegram = expanded
                                    else: website = expanded

                            # PRIORITY 2: Tweet Entities
                            if "entities" in tweet and "urls" in tweet["entities"]:
                                for url in tweet["entities"]["urls"]:
                                    expanded = url.get("expanded_url", "")
                                    if "t.me" in expanded or "telegram.me" in expanded: telegram = expanded
                                    elif not website: website = expanded
                            
                            # 3. Fallback: If has:links was true but we failed to parse a "website",
                            # just use the first link found in tweet as generic website.
                            if not website and not telegram:
                                if "entities" in tweet and "urls" in tweet["entities"]:
                                    first_url = tweet["entities"]["urls"][0].get("expanded_url")
                                    if first_url and "twitter.com" not in first_url and "x.com" not in first_url:
                                        website = first_url

                            # REQUIREMENT RELAXED: Just need a Handle + (Any Link OR decent profile)
                            # But effectively we want *some* destination.
                            if not telegram and not website: 
                                # If profile has description, we might accept it? 
                                # User said "remove telegram only".
                                # We'll accept if we have a website field (even if it's just a tweet link)
                                pass 
                            
                            # If still empty, maybe skip? 
                            # Let's be generous: If we have a username and the tweet matched "launching" + "has:links",
                            # we treat it as a lead.
                            
                            # AI Opener
                            text_lower = tweet.get("text", "").lower()
                            project_type = "project"
                            if "depin" in text_lower: project_type = "DePIN protocol"
                            elif "ai" in text_lower: project_type = "AI agent"
                            elif "nft" in text_lower: project_type = "NFT collection"
                            
                            chain = "your chain"
                            if "solana" in text_lower or " sol " in text_lower: chain = "Solana"
                            elif "base" in text_lower: chain = "Base"
                            elif "eth" in text_lower: chain = "Ethereum"
                            
                            icebreaker = f"Saw your launch announcement on X—cool {project_type} on {chain}. Let's chat partnerships?"

                            lead = RawLead(
                                name=f"@{username}",
                                source="X (Fresh)",
                                website=website,
                                twitter_handle=username,
                                profile_image_url=user.get("profile_image_url"), 
                                extra_data={
                                    "description": tweet.get("text"),
                                    "tweet_id": tweet.get("id"),
                                    "metrics": metrics,
                                    "author_desc": user.get("description"),
                                    "launch_date": tweet.get("created_at"),
                                    "telegram_channel": telegram,
                                    "tags": [project_type]
                                }
                            )
                            lead.extra_data["icebreaker"] = icebreaker
                            leads.append(lead)

                        # Pagination Logic
                        next_token = meta.get("next_token")
                        if not next_token:
                            break # No more pages
                            
                        pages_fetched += 1
                        await asyncio.sleep(1.5) # Polite paging
                        
                    except Exception as e:
                        self.logger.error(f"X API Search Error: {e}")
                        break
                    
        self.logger.info(f"✅ X Fresh Scan Complete. Yielded {len(leads)} leads.")
        return leads
