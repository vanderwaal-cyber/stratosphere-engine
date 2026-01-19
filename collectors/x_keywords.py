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

    async def collect_profiles(self, keyword: str = "", location: str = "") -> List[RawLead]:
        """
        Run a focused scrape based on provided keyword/location.
        Falls back to default queries if nothing provided.
        """
        custom_queries = []
        if keyword:
            base_q = f'"{keyword}" site:x.com'
            if location:
                base_q += f' "{location}"'
            custom_queries.append(base_q)
            custom_queries.append(f'site:x.com "{keyword}" "{location}" "founder"')
        if not custom_queries:
            custom_queries = self.queries

        original = self.queries
        self.queries = custom_queries
        try:
            return await self.collect()
        finally:
            self.queries = original

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
                    
                    # Basic Lead
                    lead = RawLead(
                        name=name,
                        source="x_signal_search",
                        profile_image_url=None,
                        extra_data={"query": q, "context": title}
                    )
                    
                    if "mirror.xyz" in link or "medium.com" in link:
                        lead.extra_data['announcement_url'] = link
                    elif "twitter.com" in link or "x.com" in link:
                        # Extract handle if direct link
                        m = re.search(r'(?:twitter\.com|x\.com)/([a-zA-Z0-9_]+)', link)
                        if m:
                            lead.twitter_handle = m.group(1)
                            lead.profile_image_url = f"https://unavatar.io/twitter/{lead.twitter_handle}"
                    
                    # Fallback avatar using initials to avoid blank UI slots
                    if not lead.profile_image_url:
                        lead.profile_image_url = f"https://ui-avatars.com/api/?name={urllib.parse.quote(name)}&background=random&color=fff"
                    
                    if lead.name and len(lead.name) > 2:
                        leads.append(lead)
                        
            except Exception as e:
                self.logger.warning(f"Query {q} failed: {e}")
                
        return leads
