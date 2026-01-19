import asyncio
import urllib.parse
import random
import time
from typing import List
from bs4 import BeautifulSoup
from collectors.base import BaseCollector, RawLead
import re

class UniversalSearchCollector(BaseCollector):
    def __init__(self):
        super().__init__("universal_search")
        self.user_agents = [
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
        ]
        
        # KEYWORD PERMUTATION ENGINE
        # We combine these to form thousands of queries
        self.niches = [
            "crypto", "defi", "web3", "memecoin", "nft", "dao", "L2", "zk", "ai agent", "depin", "rwa"
        ]
        
        self.types = [
            "protocol", "labs", "finance", "exchange", "swap", "network", "foundation", "app", "game"
        ]
        
        self.actions = [
            "waitlist", "early access", "launching soon", "airdrop confirmed", "testnet live", "beta signup"
        ]
        
        self.modifiers = [
            "site:twitter.com",
            "site:x.com"
        ]

    async def collect(self) -> List[RawLead]:
        leads = []
        try:
            # Generate 8 random query permutations per run
            queries = []
            for _ in range(8):
                niche = random.choice(self.niches)
                typ = random.choice(self.types)
                action = random.choice(self.actions)
                
                # Format: "defi protocol waitlist site:twitter.com"
                q = f"{niche} {typ} {action}"
                if random.random() > 0.3: # 70% chance to force Twitter site search
                    q += " " + random.choice(self.modifiers)
                queries.append(q)
            
            for q in queries:
                self.logger.info(f"🔎 Deep Search: '{q}'")
                
                # We scrape 1-3 pages deep per query
                current_url = f"https://html.duckduckgo.com/html/?q={urllib.parse.quote(q)}&kl=us-en"
                
                for page_num in range(1, 4):
                    # Rate limit jitter
                    await asyncio.sleep(random.uniform(2.5, 5.0))
                    
                    html = await self.fetch_page(current_url)
                    if not html or "No results" in html: break
                    
                    soup = BeautifulSoup(html, 'html.parser')
                    results = soup.find_all('div', class_='result')
                    
                    page_found = 0
                    for res in results:
                        title_tag = res.find('a', class_='result__a')
                        if not title_tag: continue
                        
                        title = title_tag.get_text(strip=True)
                        link = title_tag.get('href', '')
                        
                        # 1. Verification: MUST be a Twitter/X link if looking for handle
                        handle = None
                        if "twitter.com" in link or "x.com" in link:
                             m = re.search(r'(?:twitter\.com|x\.com)/([a-zA-Z0-9_]+)', link)
                             if m: 
                                 handle = m.group(1)
                                 if handle.lower() in ['search', 'home', 'explore', 'notifications']: continue # Skip system pages
                        
                        # 2. Heuristics for non-twitter links (project sites)
                        # Only accept if description contains strong keywords
                        
                        if not handle and "http" in link:
                             # We only want High Confidence leads now.
                             # If it's a random site, we might skip unless title is very clear
                             pass
                        
                        if handle:
                            # Clean Name
                            name = handle
                            # Try to get nicer name from title "Project Name (@handle) on X"
                            if "(" in title: 
                                name = title.split("(")[0].strip()
                            elif " on Twitter" in title:
                                name = title.split(" on Twitter")[0].strip()
                                
                            leads.append(RawLead(
                                name=name,
                                source=f"deep_search_q_{q.replace(' ', '_')[:10]}",
                                website=link,
                                twitter_handle=handle,
                                extra_data={"query": q, "title": title}
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
                    
        except Exception as e:
            self.logger.error(f"Deep Search Error: {e}")
            
        return leads
