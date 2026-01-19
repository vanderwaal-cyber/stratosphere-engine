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
        
        # 50+ Rotating Queries for Infinite Variety
        self.base_queries = [
            "new crypto project waitlist",
            "join early access web3",
            "stealth defi protocol announcing",
            "upcoming solana airdrop",
            "base chain new tokens",
            "crypto presale live now",
            "new meme coin launchpad",
            "berachain ecosystem projects",
            "monad ecosystem early",
            "crypto seed round announced",
            "web3 gaming beta signup",
            "decentralized exchange coming soon",
            "yield aggregator v2 launch",
            "ai crypto project whitepaper",
            "rwa protocol tokenization",
            "crypto infrastructure startup backed by",
            "testnet live incentivized",
            "mainnet launch countdown crypto",
            "new nft marketplace alpha",
            "web3 social app waitlist"
        ]
        
        self.modifiers = [
            "site:twitter.com",
            "site:medium.com",
            "site:mirror.xyz",
            "-site:youtube.com" # Noise
        ]

    async def collect(self) -> List[RawLead]:
        leads = []
        try:
            # Pick 5 random queries (Increased from 3)
            chosen_queries = random.sample(self.base_queries, 5)
            
            for q in chosen_queries:
                # 50% chance to add a site modifier for specificity
                if random.random() > 0.5:
                    q += " " + random.choice(self.modifiers)
                    
                self.logger.info(f"🔎 Universal Search: '{q}' (Pagination: ON)")
                
                # Pagination Loop (5 Pages Depth)
                current_url = f"https://html.duckduckgo.com/html/?q={urllib.parse.quote(q)}&kl=us-en"
                
                for page_num in range(1, 6): # Pages 1 to 5
                    self.logger.info(f"  -> Page {page_num}...")
                    
                    # Random delay
                    await asyncio.sleep(random.uniform(2.0, 4.0))
                    
                    headers = {'User-Agent': random.choice(self.user_agents)}
                    html = await self.fetch_page(current_url) 
                    
                    if not html or "No results" in html:
                        break

                    soup = BeautifulSoup(html, 'html.parser')
                    results = soup.find_all('div', class_='result')
                    
                    page_leads_count = 0
                    for res in results:
                        title_tag = res.find('a', class_='result__a')
                        snippet_tag = res.find('a', class_='result__snippet')
                        if not title_tag: continue
                        
                        title = title_tag.get_text(strip=True)
                        link = title_tag.get('href', '')
                        snippet = snippet_tag.get_text(strip=True) if snippet_tag else ""
                        
                        # 1. Parsing Logic
                        name = self._extract_name(title)
                        if not name or len(name) > 40: continue
                        
                        # 2. Heuristics
                        combined_text = (title + " " + snippet).lower()
                        keywords = ['crypto', 'web3', 'defi', 'token', 'chain', 'protocol', 'dao', 'nft', 'wallet', 'ledger']
                        if not any(k in combined_text for k in keywords):
                            continue
                            
                        # 3. Create Lead
                        lead = RawLead(
                            name=name,
                            source=f"universal_search_p{page_num}",
                            website=link if "http" in link else None,
                            extra_data={
                                "query": q,
                                "context": title,
                                "snippet": snippet[:100]
                            }
                        )
                        
                        # Handle extraction
                        if "twitter.com" in link or "x.com" in link:
                             m = re.search(r'(?:twitter\.com|x\.com)/([a-zA-Z0-9_]+)', link)
                             if m: lead.twitter_handle = m.group(1)
                             
                        leads.append(lead)
                        page_leads_count += 1
                        
                    # Find Next Page
                    # DDG HTML has a form with <input name="s"> and <input name="dc">
                    next_form = soup.find('form', action='/html/')
                    if not next_form or "next" not in next_form.get_text().lower():
                         # No next page
                         break
                         
                    # Extract hidden inputs for next page
                    inputs = next_form.find_all('input', type='hidden')
                    params = {i.get('name'): i.get('value') for i in inputs}
                    params['q'] = q # Ensure q is preserved
                    
                    # Construct next URL
                    query_string = urllib.parse.urlencode(params)
                    current_url = f"https://html.duckduckgo.com/html/?{query_string}"
                    
                    if page_leads_count == 0:
                        break # Stop if current page yielded nothing helpful
            
            self.logger.info(f"✅ Universal Search found {len(leads)} candidates")
            
        except Exception as e:
            self.logger.error(f"Universal Search failed: {e}")
            
        return leads

    def _extract_name(self, title):
        # "Monad - The high performance L1" -> "Monad"
        # "Announcing the launch of Berachain" -> "Berachain" (harder)
        
        # Simple splitters
        splitters = [':', '|', '-', '–', '—', ' announces ', ' raises ', ' launches ']
        for s in splitters:
            if s in title:
                parts = title.split(s)
                # Usually the Name is the first part, unless it's "Announcing X"
                candidate = parts[0].strip()
                if len(candidate.split()) < 5:
                     return candidate
        
        # Fallback: Just take first 3 words
        words = title.split()
        if len(words) <= 3:
            return title
            
        return None
