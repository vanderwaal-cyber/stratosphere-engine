import urllib.parse
from bs4 import BeautifulSoup
from typing import List
from collectors.base import BaseCollector, RawLead
import re

class XKeywordCollector(BaseCollector):
    def __init__(self):
        super().__init__("x_signals")
        # High Intent Queries
        self.queries = [
            'site:mirror.xyz "announcing our seed round"',
            'site:medium.com "we are excited to announce" protocol',
            'site:x.com "raised" AND "pre-seed" -bitcoin',
            'site:x.com "backed by" AND "defiance" -crypto',
            '"mainnet launch" protocol incentivized'
        ]

    async def collect(self) -> List[RawLead]:
        leads = []
        for q in self.queries:
            try:
                # DuckDuckGo Lite Scrape
                encoded = urllib.parse.quote(q)
                url = f"https://html.duckduckgo.com/html/?q={encoded}&kl=us-en"
                
                html = await self.fetch_page(url)
                soup = BeautifulSoup(html, 'html.parser')
                results = soup.find_all('div', class_='result')
                
                for res in results:
                    title_tag = res.find('a', class_='result__a')
                    if not title_tag: continue
                    
                    title = title_tag.get_text(strip=True)
                    link = title_tag.get('href', '')
                    
                    # Clean Name
                    # E.g. "Announcing Monad: The ... | Mirror" -> Monad
                    name = title.split(':')[0].split('|')[0].strip()
                    if len(name) > 30: continue # Likely not a name
                    
                    # Score it
                    score = 10 # Base
                    full_text = title.lower()
                    if "announced" in full_text or "launch" in full_text: score += 20
                    if "raise" in full_text or "backed" in full_text: score += 30
                    
                    # Basic Lead
                    lead = RawLead(
                        name=name,
                        source="x_signal_search",
                        extra_data={"query": q, "context": title, "activity_score": score}
                    )
                    
                    if "mirror.xyz" in link or "medium.com" in link:
                        lead.extra_data['announcement_url'] = link
                    elif "twitter.com" in link or "x.com" in link:
                        # Extract handle if direct link
                        m = re.search(r'(?:twitter\.com|x\.com)/([a-zA-Z0-9_]+)', link)
                        if m: lead.twitter_handle = m.group(1)
                    
                    if lead.name and len(lead.name) > 2:
                        leads.append(lead)
                        
            except Exception as e:
                self.logger.warning(f"Query {q} failed: {e}")
                
        return leads
