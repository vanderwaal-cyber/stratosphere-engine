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
        # High signal-to-noise ratio filters
        queries = [
            # 1. New Projects / Launches (High Velocity)
            '(("new protocol" OR "launching" OR "IDO" OR "TGE" OR "mainnet" OR "testnet") (crypto OR defi OR depin OR ai OR gamefi) min_faves:5 -is:retweet has:links)',
            
            # 2. Specific Narratives (AI/DePIN focus)
            '(("AI agent" OR "DePIN" OR "privacy coin") (launch OR announcement) min_faves:2 -is:retweet)',
            
            # 3. Contract/Ca Drops (Degen/Memecoin potential)
            '(("contract address" OR "ca:") (solana OR eth OR base) min_faves:10 -is:retweet)'
        ]

        async with httpx.AsyncClient(headers={"Authorization": f"Bearer {self.bearer_token}"}, timeout=30) as client:
            for query in queries:
                try:
                    self.logger.info(f"Searching X for: {query[:50]}...")
                    resp = await client.get(
                        f"{self.base_url}/tweets/search/recent",
                        params={
                            "query": query,
                            "max_results": 50, # 50-200 fresh per scan
                            "tweet.fields": "created_at,author_id,entities,public_metrics,text",
                            "expansions": "author_id",
                            "user.fields": "username,description,url,entities,public_metrics"
                        }
                    )
                    
                    if resp.status_code == 429:
                        self.logger.warning("X API Rate Limit hit. Pausing...")
                        break
                        
                    if resp.status_code != 200:
                        self.logger.error(f"X API Error {resp.status_code}: {resp.text}")
                        continue
                        
                    data = resp.json()
                    tweets = data.get("data", [])
                    users = {u["id"]: u for u in data.get("includes", {}).get("users", [])}
                    
                    for tweet in tweets:
                        author_id = tweet.get("author_id")
                        user = users.get(author_id, {})
                        username = user.get("username")
                        
                        if not username: continue
                        
                        # Metrics for Scoring
                        metrics = tweet.get("public_metrics", {})
                        likes = metrics.get("like_count", 0)
                        replies = metrics.get("reply_count", 0)
                        
                        # Extract Links
                        website = None
                        telegram = None
                        
                        # Check user profile links (Highest Priority)
                        if "entities" in user and "url" in user["entities"]:
                            for url in user["entities"]["url"].get("urls", []):
                                expanded = url.get("expanded_url", "")
                                if "t.me" in expanded or "telegram.me" in expanded: telegram = expanded
                                else: website = expanded

                        # Check tweet links
                        if "entities" in tweet and "urls" in tweet["entities"]:
                            for url in tweet["entities"]["urls"]:
                                expanded = url.get("expanded_url", "")
                                if "t.me" in expanded or "telegram.me" in expanded: telegram = expanded
                                elif not website: website = expanded

                        # AI Opener Generation (Template Based)
                        text_lower = tweet.get("text", "").lower()
                        opener_category = "project"
                        if "depin" in text_lower: opener_category = "DePIN protocol"
                        elif "ai" in text_lower: opener_category = "AI agent"
                        elif "gamefi" in text_lower: opener_category = "GameFi project"
                        elif "privacy" in text_lower: opener_category = "privacy tech"
                        
                        icebreaker = f"Hey, saw your new {opener_category} launch on X! The engagement looks great ({likes} likes). Are you looking for partners?"

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
                                "telegram_channel": telegram
                            }
                        )
                        # Attach pre-generated icebreaker to be saved
                        lead.extra_data["icebreaker"] = icebreaker
                        
                        leads.append(lead)
                        
                except Exception as e:
                    self.logger.error(f"X API Search Error: {e}")
                    
        return leads
