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

        queries = [
            '(("new project" OR "launching" OR "live now" OR "IDO" OR "TGE") (crypto OR defi OR nft OR depin OR ai) has:links -is:retweet)',
            '(("contract address" OR "ca:") (solana OR eth OR base) -is:retweet)'
        ]

        async with httpx.AsyncClient(headers={"Authorization": f"Bearer {self.bearer_token}"}, timeout=20) as client:
            for query in queries:
                try:
                    self.logger.info(f"Searching X for: {query[:30]}...")
                    resp = await client.get(
                        f"{self.base_url}/tweets/search/recent",
                        params={
                            "query": query,
                            "max_results": 100,
                            "tweet.fields": "created_at,author_id,entities",
                            "expansions": "author_id",
                            "user.fields": "username,description,url,entities"
                        }
                    )
                    
                    if resp.status_code == 429:
                        self.logger.warning("X API Rate Limit hit.")
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
                        
                        # Extract Links
                        website = None
                        telegram = None
                        
                        # Check user profile links
                        if "entities" in user and "url" in user["entities"]:
                            for url in user["entities"]["url"].get("urls", []):
                                expanded = url.get("expanded_url", "")
                                if "t.me" in expanded: telegram = expanded
                                else: website = expanded

                        # Check tweet links
                        if "entities" in tweet and "urls" in tweet["entities"]:
                            for url in tweet["entities"]["urls"]:
                                expanded = url.get("expanded_url", "")
                                if "t.me" in expanded: telegram = expanded
                                elif not website: website = expanded

                        lead = RawLead(
                            name=f"@{username} Project",
                            source="x_api_search",
                            website=website,
                            twitter_handle=username,
                            extra_data={
                                "description": tweet.get("text"),
                                "tweet_id": tweet.get("id"),
                                "author_desc": user.get("description"),
                                "launch_date": tweet.get("created_at"),
                                "telegram_channel": telegram
                            }
                        )
                        leads.append(lead)
                        
                except Exception as e:
                    self.logger.error(f"X API Search Error: {e}")
                    
        return leads
