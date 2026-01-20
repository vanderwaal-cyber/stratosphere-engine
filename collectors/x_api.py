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

        # Phoenix-inspired "Out-of-Network" Discovery Queries
        # 20+ High-Signal Queries to flood the pipeline with FRESH leads
        queries = [
            # --- SECTOR 1: LAUNCH & PRESALE (High Velocity) ---
            '(("fair launch" OR "stealth launch" OR "presale live" OR "IDO" OR "TGE") (crypto OR defi OR solana OR base chain) min_faves:2 -is:retweet has:links)',
            '(("whitelist open" OR "beta access" OR "early access") (crypto OR "app chain") min_faves:2 -is:retweet)',
            '(("contract address" OR "ca:") (solana OR eth OR base) min_faves:5 -is:retweet)',
            
            # --- SECTOR 2: NARRATIVES (DePIN / AI / RWA) ---
            '(("DePIN" OR "Decentralized Physical Infrastructure") ("launching" OR "roadmap" OR "building") min_faves:5 -is:retweet)',
            '(("AI Agent" OR "Autonomous Agent" OR "on-chain ai") ("live" OR "demo" OR "testing") min_faves:5 -is:retweet)',
            '(("RWA" OR "Real World Assets") ("tokenizing" OR "live") min_faves:5 -is:retweet)',
            
            # --- SECTOR 3: GROWTH HACKS (Partnerships / Grants) ---
            '(("we are hiring" OR "looking for builders") (web3 OR crypto) min_faves:5 -is:retweet)',
            '(("grant program" OR "hackathon winner") (solana OR eth) min_faves:10 -is:retweet)',
            
            # --- SECTOR 4: DISCOVERY BOT SIGNALS ---
            '(("new listing" OR "just listed") (coingecko OR coinmarketcap) min_faves:10 -is:retweet)'
        ]

        # Use a random subset if we have too many, or run all if within limits.
        # Running all 9 queries * 50 results = 450 leads max per run.
        
        async with httpx.AsyncClient(headers={"Authorization": f"Bearer {self.bearer_token}"}, timeout=45) as client:
            for query in queries:
                try:
                    self.logger.info(f"🔎 Phoenix-Scanning X for: {query[:40]}...")
                    resp = await client.get(
                        f"{self.base_url}/tweets/search/recent",
                        params={
                            "query": query,
                            "max_results": 60, # Bumped to 60 per query x 9 queries = ~540 potential raw leads
                            "tweet.fields": "created_at,author_id,entities,public_metrics,text",
                            "expansions": "author_id",
                            "user.fields": "username,description,url,entities,public_metrics"
                        }
                    )
                    
                    if resp.status_code == 429:
                        self.logger.warning("X API Rate Limit hit. Cooling down...")
                        break # Stop this run, resume next time
                        
                    if resp.status_code != 200:
                        self.logger.error(f"X API Error {resp.status_code}: {resp.text}")
                        continue
                        
                    data = resp.json()
                    tweets = data.get("data", [])
                    users = {u["id"]: u for u in data.get("includes", {}).get("users", [])}
                    
                    self.logger.info(f"   -> Found {len(tweets)} raw signals.")
                    
                    for tweet in tweets:
                        author_id = tweet.get("author_id")
                        user = users.get(author_id, {})
                        username = user.get("username")
                        
                        if not username: continue
                        
                        # Metrics for Scoring
                        metrics = tweet.get("public_metrics", {})
                        likes = metrics.get("like_count", 0)
                        
                        # Extract Links (Deep Search)
                        website = None
                        telegram = None
                        
                        # PRIORITY 1: User Profile Entities
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

                        # AI Opener (Context Aware)
                        text_lower = tweet.get("text", "").lower()
                        opener_category = "project"
                        if "depin" in text_lower: opener_category = "DePIN protocol"
                        elif "ai" in text_lower: opener_category = "AI agent"
                        elif "meme" in text_lower: opener_category = "community token"
                        elif "presale" in text_lower: opener_category = "presale"
                        
                        icebreaker = f"Hey, saw your new {opener_category} update on X! Engagement looks solid ({likes} likes). Are you looking for growth partners?"

                        lead = RawLead(
                            name=f"@{username} (X)",
                            source="x_api",
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
                                "tags": [opener_category]
                            }
                        )
                        lead.extra_data["icebreaker"] = icebreaker
                        
                        leads.append(lead)
                        
                except Exception as e:
                    self.logger.error(f"X API Search Error: {e}")
                    
        self.logger.info(f"✅ X API Collection Complete. Yielded {len(leads)} raw leads.")
        return leads
