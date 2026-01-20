
import os
import asyncio
import json
from typing import List
from apify_client import ApifyClient
from collectors.base import BaseCollector, RawLead

class ApifyXCollector(BaseCollector):
    def __init__(self):
        super().__init__("x_apify")
        self.api_token = os.getenv("APIFY_API_TOKEN")
        self.client = None
        if self.api_token:
            self.client = ApifyClient(self.api_token)

    async def collect(self) -> List[RawLead]:
        leads = []
        if not self.client:
            self.logger.warning("⚠️ APIFY_API_TOKEN not found. Skipping Apify X Scrape.")
            return []

        self.logger.info("Starting Apify X Scrape (Phoenix Mode)...")

        # Queries matching the user's "Broad X Mode"
        # We can be aggressive here because Apify handles the browsing.
        queries = [
            "launching (solana OR eth OR base) has:links",
            "new protocol (solana OR eth OR base) has:links",
            "AI Agent (launching OR live) has:links",
            "DePIN (launching OR roadmap) has:links",
            "contract address (solana OR eth OR base) has:links"
        ]
        
        # We will use 'apidojo/tweet-scraper' or similar.
        # Verified Actor: 'apidojo/tweet-scraper' is reliable but 'quacker/twitter-scraper' is also good.
        # Let's use the standard "Search" input for a scraper.
        # Using "apidojo/tweet-scraper" (popular).
        
        # Reverting to 'apidojo/tweet-scraper' (ID: 61RPP7dywgiy0JPD0) now that user has PAID plan.
        run_input = {
            "queries": queries,
            "maxItems": 500, # Aiming for user's goal of 100+ leads
            "sort": "Latest",
            "tweetLanguage": "en"
        }

        try:
            self.logger.info("   -> Sending job to Apify (apidojo/tweet-scraper) [PAID PLAN ACTIVE] ...")
            
            def run_actor():
                run = self.client.actor("61RPP7dywgiy0JPD0").call(run_input=run_input)
                return run

            run = await asyncio.to_thread(run_actor)
            
            dataset_id = run["defaultDatasetId"]
            self.logger.info(f"   -> Job Finished. Fetching results from Dataset {dataset_id}...")
            
            # Fetch results
            # item_iterator = self.client.dataset(dataset_id).iterate_items()
            # Convert to list
            def get_items():
                return list(self.client.dataset(dataset_id).iterate_items())
            
            items = await asyncio.to_thread(get_items)
            
            self.logger.info(f"   -> Retrieved {len(items)} raw tweets from Apify.")

            for item in items:
                # Map Apify JSON to RawLead
                # Structure varies, but usually:
                # text, author, createdAt, etc.
                
                try:
                    text = item.get("text", "") or item.get("fullText", "")
                    user_data = item.get("author", {}) or item.get("user", {})
                    username = user_data.get("userName") or user_data.get("screen_name")
                    
                    if not username: continue
                    
                    # Links
                    website = None
                    telegram = None
                    
                    # Extract from text/entities if available
                    # The actor usually returns parsed entities
                    
                    # Simple regex fallback if structured extraction missing
                    import re
                    
                    # Try to find Telegram
                    if "t.me" in text or "telegram.me" in text:
                        # Extract first t.me link
                        match = re.search(r"(https?://(?:t\.me|telegram\.me)/[\w_]+)", text)
                        if match: telegram = match.group(0)
                        
                    # Try to find generic link
                    # (Apify often puts them in 'entities.urls')
                    # We'll rely on text parsing for robustness if structure varies
                    
                    if not telegram and "http" in text:
                         match = re.search(r"(https?://[^\s]+)", text)
                         if match: 
                             url = match.group(0)
                             if "t.me" not in url and "twitter.com" not in url and "x.com" not in url:
                                 website = url
                    
                    # AI Opener
                    project_type = "project"
                    if "depin" in text.lower(): project_type = "DePIN protocol"
                    elif "ai" in text.lower(): project_type = "AI agent"
                    
                    lead = RawLead(
                        name=f"@{username}",
                        source="Apify (X)",
                        website=website,
                        twitter_handle=username,
                        profile_image_url=user_data.get("profileImageUrl"),
                        extra_data={
                            "description": text,
                            "telegram_channel": telegram,
                            "metrics": {
                                "likes": item.get("likeCount", 0),
                                "replies": item.get("replyCount", 0),
                                "retweets": item.get("retweetCount", 0)
                            },
                            "launch_date": item.get("createdAt"),
                            "url": item.get("url") # Tweet URL
                        }
                    )
                    lead.extra_data["icebreaker"] = f"Saw your {project_type} post on X. Open to partnerships?"
                    leads.append(lead)
                    
                except Exception as e:
                    continue

        except Exception as e:
            self.logger.error(f"Apify Error: {e}")
            
        return leads
