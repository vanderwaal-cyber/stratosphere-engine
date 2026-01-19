import asyncio
import random
from typing import List
from bs4 import BeautifulSoup
from collectors.base import BaseCollector, RawLead

class LaunchpadCollector(BaseCollector):
    def __init__(self):
        super().__init__("launchpad_aggregator")
        # Reliable aggregators that are easy to parse
        self.sources = [
            "https://icodrops.com/category/upcoming-ico/",
            "https://icodrops.com/category/active-ico/"
        ]

    async def collect(self) -> List[RawLead]:
        leads = []
        try:
            for url in self.sources:
                self.logger.info(f"🚀 Scraping Launchpad: {url}")
                await asyncio.sleep(random.uniform(2.0, 5.0))
                
                html = await self.fetch_page(url)
                if not html: continue
                
                soup = BeautifulSoup(html, 'html.parser')
                
                # ICO Drops Specific Structure
                cards = soup.find_all('div', class_='ico-main-info')
                
                for card in cards:
                    h3 = card.find('h3')
                    if not h3: continue
                    
                    name_tag = h3.find('a')
                    if not name_tag: continue
                    
                    name = name_tag.get_text(strip=True)
                    detail_url = name_tag.get('href')
                    
                    # Validating
                    if not name or len(name) < 2: continue
                    
                    # Description
                    desc_div = card.find('div', class_='ico-description')
                    desc = desc_div.get_text(strip=True) if desc_div else "Upcoming Launch"
                    
                    leads.append(RawLead(
                        name=name,
                        source="icodrops",
                        website=detail_url, # Usually points to ICO drops detail page, enriched later
                        extra_data={
                            "desc": desc,
                            "stage": "Pre-Launch"
                        }
                    ))
            
            self.logger.info(f"✅ Launchpad found {len(leads)} projects")
            
        except Exception as e:
            self.logger.error(f"Launchpad Collector failed: {e}")
            
        return leads
