
import os
import asyncio
import json
from typing import List
from core.config import get_settings
from apify_client import ApifyClient
from collectors.base import BaseCollector, RawLead

class ApifyXCollector(BaseCollector):
    def __init__(self):
        super().__init__("x_apify")
        self.settings = get_settings()
        self.api_token = self.settings.APIFY_API_TOKEN
        self.client = None
        if self.api_token:
            self.client = ApifyClient(self.api_token)

    async def collect(self) -> List[RawLead]:
        leads = []
        if not self.client:
            self.logger.warning("⚠️ APIFY_API_TOKEN not found. Skipping Apify X Scrape.")
            return []

        self.logger.info("Starting Apify X Scrape (Phoenix Mode)...")

        # DYNAMIC KEYWORD SYSTEM (Phoenix Logic)
        # Randomly select topics to ensure variety every run
        import random
        
        sectors = [
            "DePIN", "Real World Assets", "RWA", "AI Agent", "GameFi", 
            "ZK Rollup", "Layer3", "SocialFi", "Decentralized Science", "DeSci",
            "Perp Dex", "Yield Optimizer", "Restaking", "EigenLayer", "Liquid Staking",
            "Privacy Protocol", "On-chain Gaming", "Prediction Market", "Consumer Crypto"
        ]
        
        actions = [
            "launching", "announced", "live", "beta", "whitelist", "presale", 
            "testnet", "mainnet", "airdrop", "early access", "waitlist"
        ]
        
        networks = [
            "Solana", "Base", "Arbitrum", "Monad", "Berachain", "Sei", "Sui", "Aptos"
        ]
        
        # Generator: Create 5 unique comprehensive queries
        queries = []
        for _ in range(5):
            sector = random.choice(sectors)
            action = random.choice(actions)
            # 50% chance to append a network for specificity
            # RELAXED: Removed quotes to broaden search matches
            # RELAXED: Removed 'has:links' from some queries to catch text-only announcements
            
            if random.random() > 0.5:
                query = f"{sector} {action}"  # Broadest
            else:
                query = f"{sector} {action} has:links" # Specific
                
            if random.random() > 0.7:
                 net = random.choice(networks)
                 query += f" {net}"
            
            queries.append(query)
            
        # Add one "Safety Net" query to GUARANTEE results
        queries.append("crypto launching")
        queries.append("new crypto project")
        
        self.logger.info(f"Generated Dynamic Queries: {queries}")

        run_input = {
            "searchTerms": queries, 
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
            print(f"DEBUG: Dataset ID: {dataset_id}") # Production Trace
            
            # Fetch results
            def get_items():
                items = list(self.client.dataset(dataset_id).iterate_items())
                print(f"DEBUG: Items fetched count: {len(items)}") # Production Trace
                if len(items) > 0:
                    print(f"DEBUG: First Item Keys: {items[0].keys()}")
                return items
            
            items = await asyncio.to_thread(get_items)
            
            self.logger.info(f"   -> Retrieved {len(items)} raw tweets from Apify.")

            for item in items:
                # Map Apify JSON to RawLead
                # Structure varies, but usually:
                # text, author, createdAt, etc.
                
                try:
                    text = item.get("text", "") or item.get("fullText", "")
                    
                    # Robust Username Extraction (Handles Simple + GraphQL formats)
                    user_data = item.get("author", {}) or item.get("user", {})
                    username = user_data.get("userName") or user_data.get("screen_name") or user_data.get("username")
                    
                    # Fallback: Deep GraphQL Extraction (item -> core -> user_results -> result -> legacy -> screen_name)
                    if not username:
                        try:
                            # Try standard GraphQL path
                            core = item.get("core", {})
                            res = core.get("user_results", {}).get("result", {})
                            username = res.get("legacy", {}).get("screen_name")
                        except: pass
                        
                    if not username:
                        try:
                            # Try nested 'user' object structure seen in recent Apify updates
                            legacy = item.get("user", {}).get("result", {}).get("legacy", {})
                            username = legacy.get("screen_name")
                        except: pass

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
