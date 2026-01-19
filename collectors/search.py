import asyncio
import random
import re
from typing import List
from duckduckgo_search import DDGS
from collectors.base import BaseCollector, RawLead

class UniversalSearchCollector(BaseCollector):
    def __init__(self):
        super().__init__("universal_search")
        
        # CT RADAR: Expanded Keywords & Recency
        self.ecosystems = [
            "solana", "ethereum", "base chain", "arbitrum", "monad", "berachain", "blast", "optimism", "zkSync", "sui", "sei", "aptos", 
            "avalanche", "polygon", "mantle", "linea", "scroll", "starknet"
        ]
        self.niches = [
            "defi", "web3", "memecoin", "nft", "dao", "L2", "zk", "ai agent", "depin", "rwa", "gaming", "socialfi", "perp dex", "lending", 
            "yield", "bridge", "wallet", "infra", "auditor", "launcher"
        ]
        self.types = [
            "protocol", "labs", "finance", "exchange", "swap", "network", "foundation", "app", "game", "infra", "studio", "ventures"
        ]
        self.actions = [
            "waitlist", "early access", "launching soon", "airdrop confirmed", "testnet live", "beta signup", "presale", "whitelist",
            "mainnet", "v2 live", "v3 launch", "roadmap update", "we represent", "building on"
        ]
        
        # RECENCY BIAS: Force search engines to surface recent content
        self.recency_markers = [
            "2024", "2025", "this week", "Q1 2025", "just launched", "live now"
        ]
        
        self.modifiers = ["site:twitter.com", "site:x.com"]

    async def collect(self, query_override: List[str] = None) -> List[RawLead]:
        leads = []
        try:
            # Check for override (from Engine rotation)
            if query_override:
                queries = list(set(query_override))
            else:
                # Generate 20 unique queries per batch run (Default CT Radar Mode)
                queries = set()
                while len(queries) < 20:
                    eco = random.choice(self.ecosystems) if random.random() > 0.4 else ""
                    niche = random.choice(self.niches)
                    typ = random.choice(self.types)
                    action = random.choice(self.actions)
                    recency = random.choice(self.recency_markers) if random.random() > 0.6 else ""
                    
                    # Permutation: "solana defi protocol waitlist 2025"
                    parts = [p for p in [eco, niche, typ, action, recency] if p]
                    q = " ".join(parts)
                    
                    # 80% chance to force Twitter site search (CT Radar Mode)
                    if random.random() > 0.2: 
                        q += " " + random.choice(self.modifiers)
                    queries.add(q)
                
                queries = list(queries)
            
            self.logger.info(f"🔎 Running {len(queries)} search queries...")
            
            for i, q in enumerate(queries):
                self.logger.info(f"📡 Query ({i+1}/{len(queries)}): '{q}'")
                
                try:
                    # Use DDGS library - much more reliable than HTML scraping
                    with DDGS() as ddgs:
                        results = list(ddgs.text(q, max_results=30))
                    
                    if not results:
                        self.logger.warning(f"No results for: {q}")
                        continue
                    
                    for res in results:
                        title = res.get('title', '')
                        link = res.get('href', '')
                        snippet = res.get('body', '')
                        full_text = (title + " " + snippet).lower()
                        
                        # Extract Twitter handle
                        handle = None
                        
                        # Strategy 1: Link is Twitter
                        if "twitter.com" in link or "x.com" in link:
                            m = re.search(r'(?:twitter\.com|x\.com)/([a-zA-Z0-9_]+)', link)
                            if m: 
                                handle = m.group(1)
                        
                        # Strategy 2: Title contains @handle
                        if not handle and "@" in title:
                            try:
                                words = title.split()
                                for w in words:
                                    if w.startswith("@") and len(w) > 3:
                                        handle = w.replace("@", "").replace(")", "").replace(",", "")
                                        break
                            except: 
                                pass
                        
                        if handle:
                            # Filter out common Twitter pages
                            if handle.lower() in ['search', 'home', 'explore', 'notifications', 'hashtag', 'status', 'i', 'intent', 'share']:
                                continue
                            
                            # Clean Name
                            name = handle
                            if "(" in title: 
                                name = title.split("(")[0].strip()
                            elif " on " in title: 
                                name = title.split(" on ")[0].strip()
                            
                            # ACTIVITY SCORE: Simple heuristic
                            score = 0
                            if any(r in full_text for r in self.recency_markers): 
                                score += 30
                            if any(a in full_text for a in ["launch", "live", "mainnet", "beta"]): 
                                score += 20
                            if any(e in full_text for e in self.ecosystems): 
                                score += 10
                            
                            leads.append(RawLead(
                                name=name,
                                source=f"ct_radar",
                                website=link,
                                twitter_handle=handle,
                                extra_data={
                                    "query": q, 
                                    "title": title, 
                                    "activity_score": score,
                                    "snippet": snippet[:100]
                                }
                            ))
                    
                    # Small delay between queries to avoid hammering
                    await asyncio.sleep(random.uniform(1, 2))
                    
                except Exception as e:
                    self.logger.error(f"Query failed '{q}': {e}")
                    continue
            
            self.logger.info(f"✅ Search Complete. Found {len(leads)} raw leads.")
                    
        except Exception as e:
            self.logger.error(f"Deep Search Error: {e}")
            
        return leads
