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

        # "X Fresh Scanner" - Primary Source Logic
        # Prioritize tweets that explicitly signal a Telegram group => High Intent
        queries = [
            # 1. THE MASTER QUERY (User Spec: Launch + Telegram Link + Engagement)
            '(("launching" OR "IDO" OR "TGE" OR "testnet" OR "DePIN" OR "AI agent" OR "NFT drop" OR "new protocol") (telegram OR t.me) has:links -is:retweet min_faves:3 min_replies:2)',
            
            # 2. Backup: Broad "New Project" discovery (High Precision)
            '(("fair launch" OR "stealth launch" OR "contract address") (solana OR eth OR base) (t.me OR telegram) -is:retweet min_faves:3)',
            
            # 3. Narrative Specific (DePIN/AI)
            '(("DePIN" OR "AI Agent") ("roadmap" OR "whitepaper") (t.me OR telegram) -is:retweet min_faves:3)'
        ]

        async with httpx.AsyncClient(headers={"Authorization": f"Bearer {self.bearer_token}"}, timeout=45) as client:
            for query in queries:
                try:
                    self.logger.info(f"🔎 X Fresh Scan: {query[:50]}...")
                    resp = await client.get(
                        f"{self.base_url}/tweets/search/recent",
                        params={
                            "query": query,
                            "max_results": 100, # Per User Request: 100-200 posts per scan
                            "tweet.fields": "created_at,author_id,entities,public_metrics,text",
                            "expansions": "author_id",
                            "user.fields": "username,description,url,entities,public_metrics"
                        }
                    )
                    
                    if resp.status_code == 429:
                        self.logger.warning("X API Rate Limit hit. Cooling down...")
                        break 
                        
                    if resp.status_code != 200:
                        self.logger.error(f"X API Error {resp.status_code}: {resp.text}")
                        continue
                        
                    data = resp.json()
                    tweets = data.get("data", [])
                    users = {u["id"]: u for u in data.get("includes", {}).get("users", [])}
                    
                    self.logger.info(f"   -> Found {len(tweets)} candidates.")
                    
                    for tweet in tweets:
                        author_id = tweet.get("author_id")
                        user = users.get(author_id, {})
                        username = user.get("username")
                        
                        if not username: continue
                        
                        # Metrics
                        metrics = tweet.get("public_metrics", {})
                        
                        # Extract Links (Deep Search)
                        website = None
                        telegram = None
                        
                        # PRIORITY 1: User Profile Entities
                        if "entities" in user and "url" in user["entities"]:
                            for url in user["entities"]["url"].get("urls", []):
                                expanded = url.get("expanded_url", "")
                                if "t.me" in expanded or "telegram.me" in expanded: telegram = expanded
                                else: website = expanded

                        # PRIORITY 2: Tweet Entities (Override if found in tweet text, likely specific to the project)
                        if "entities" in tweet and "urls" in tweet["entities"]:
                            for url in tweet["entities"]["urls"]:
                                expanded = url.get("expanded_url", "")
                                if "t.me" in expanded or "telegram.me" in expanded: telegram = expanded
                                elif not website: website = expanded
                        
                        # REQUIREMENT: Only add if we have contact info (Telegram OR Website)
                        # Given strict query, we likely have Telegram, but double check.
                        if not telegram and not website:
                            continue

                        # Generate AI Opener
                        # Template: "Saw your launch announcement on X—cool [project] on [chain]. Let's chat Telegram partnerships?"
                        text_lower = tweet.get("text", "").lower()
                        project_type = "project"
                        if "depin" in text_lower: project_type = "DePIN protocol"
                        elif "ai" in text_lower: project_type = "AI agent"
                        elif "nft" in text_lower: project_type = "NFT collection"
                        
                        chain = "your chain"
                        if "solana" in text_lower or " sol " in text_lower: chain = "Solana"
                        elif "base" in text_lower: chain = "Base"
                        elif "eth" in text_lower: chain = "Ethereum"
                        
                        icebreaker = f"Saw your launch announcement on X—cool {project_type} on {chain}. Let's chat Telegram partnerships?"

                        lead = RawLead(
                            name=f"@{username}",
                            source="X (Fresh)", # Dashboard Label
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
                        
                except Exception as e:
                    self.logger.error(f"X API Search Error: {e}")
                    
        self.logger.info(f"✅ X Fresh Scan Complete. Yielded {len(leads)} leads.")
        return leads
