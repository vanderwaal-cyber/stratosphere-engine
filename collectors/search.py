import asyncio
import urllib.parse
import random
import time
from typing import List
from bs4 import BeautifulSoup
from collectors.base import BaseCollector, RawLead
import re
import datetime

class UniversalSearchCollector(BaseCollector):
    def __init__(self):
        super().__init__("universal_search")
        self.user_agents = [
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
        ]
        
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
                queries = set(query_override)
            else:
                # Generate 50 unique queries per batch run (Default CT Radar Mode)
                queries = set()
                while len(queries) < 50:
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
            
            queries = list(queries)
            
            for i, q in enumerate(queries):
                # UI Feedback via callback if provided (e.g. "Scanning: Solana Defi (1/50)")
                if progress_callback:
                    progress_callback(step=f"Scanning: '{q}' ({i+1}/{len(queries)})")
                    
                self.logger.info(f"📡 CT Radar ({i+1}/50): '{q}'")
                
                # Robust Scrape: Use html.duckduckgo.com with random sleep buffer
                # Fallback to standard duckduckgo query param structure if needed
                current_url = f"https://html.duckduckgo.com/html/?q={urllib.parse.quote(q)}&kl=us-en"
                
                for page_num in range(1, 3):
                    await asyncio.sleep(random.uniform(2.0, 4.0)) # Slower to avoid 403
                    
                    html = await self.fetch_page(current_url)
                    
                    # BLOCKING DETECTION
                    if not html: 
                        self.logger.warning(f"Empty HTML for {q}")
                        break
                    if "If this error persists" in html or "Rate limit" in html:
                        self.logger.error("⚠️ Rate Limit Detected. Cooling down...")
                        await asyncio.sleep(5)
                        break
                        
                    if "No results" in html: break
                    
                    soup = BeautifulSoup(html, 'html.parser')
                    results = soup.find_all('div', class_='result')
                    
                    if not results:
                        # Try fallback parsing for different DDG layout
                        results = soup.find_all('div', class_='web-result')
                    
                    page_found = 0
                    for res in results:
                        # Try multiple selector strategies
                        title_tag = res.find('a', class_='result__a') or res.find('h2')
                        snippet_tag = res.find('a', class_='result__snippet') or res.find('div', class_='result__snippet')
                        
                        if not title_tag: continue
                        
                        title = title_tag.get_text(strip=True)
                        link = title_tag.get('href', '')
                        snippet = snippet_tag.get_text(strip=True) if snippet_tag else ""
                        full_text = (title + " " + snippet).lower()

                        # Logic: If query has "twitter", accept any result that looks like a project
                        handle = None
                        
                        # Strategy 1: Link is Twitter
                        if "twitter.com" in link or "x.com" in link:
                             m = re.search(r'(?:twitter\.com|x\.com)/([a-zA-Z0-9_]+)', link)
                             if m: handle = m.group(1)

                        # Strategy 2: Title contains @handle
                        if not handle and "@" in title:
                            try:
                                words = title.split()
                                for w in words:
                                    if w.startswith("@") and len(w) > 3:
                                        handle = w.replace("@", "").replace(")", "")
                                        break
                            except: pass

                        if handle:
                            if handle.lower() in ['search', 'home', 'explore', 'notifications', 'hashtag', 'status', 'i', 'intent', 'share']: continue 
                            
                            # Clean Name
                            name = handle
                            if "(" in title: name = title.split("(")[0].strip()
                            elif " on " in title: name = title.split(" on ")[0].strip() 
                        
                        if handle:
                            # Clean Name
                            name = handle
                            if "(" in title: name = title.split("(")[0].strip()
                            elif " on " in title: name = title.split(" on ")[0].strip()
                            
                            # ACTIVITY SCORE: Simple heuristic
                            score = 0
                            if any(r in full_text for r in self.recency_markers): score += 30
                            if any(a in full_text for a in ["launch", "live", "mainnet", "beta"]): score += 20
                            if any(e in full_text for e in self.ecosystems): score += 10
                            
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
                            page_found += 1
                            
                    # Next Page
                    next_form = soup.find('form', action='/html/')
                    if not next_form: break
                    inputs = next_form.find_all('input', type='hidden')
                    params = {i.get('name'): i.get('value') for i in inputs}
                    params['q'] = q 
                    current_url = f"https://html.duckduckgo.com/html/?{urllib.parse.urlencode(params)}"
                    
                    if page_found == 0: break
            
            self.logger.info(f"✅ Batch Complete. Found {len(leads)} raw leads.")
                    
        except Exception as e:
            self.logger.error(f"Deep Search Error: {e}")
            
        return leads
