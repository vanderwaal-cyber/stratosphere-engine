import asyncio
from typing import List
from bs4 import BeautifulSoup
from collectors.base import BaseCollector, RawLead
import httpx
import re

class ICOCalendarCollector(BaseCollector):
    def __init__(self):
        super().__init__("ico_calendars")
        self.sources = [
            # {"name": "icodrops", "url": "https://icodrops.com/category/upcoming-ico/"},
            # {"name": "icodrops_active", "url": "https://icodrops.com/category/active-ico/"},
            # Starting with just one robust one to test
             {"name": "icodrops", "url": "https://icodrops.com/category/upcoming-ico/"}
        ]

    async def collect(self) -> List[RawLead]:
        leads = []
        
        # 1. ICO Drops
        try:
            self.logger.info("Scraping ICO Drops...")
            html = await self.fetch_page("https://icodrops.com/category/upcoming-ico/")
            soup = BeautifulSoup(html, "html.parser")
            ico_cards = soup.select("div.a_ico")
            
            for card in ico_cards:
                try:
                    name_tag = card.select_one("h3 a")
                    if not name_tag: continue
                    name = name_tag.get_text(strip=True)
                    detail_url = name_tag['href']
                    
                    self.logger.info(f"Fetching details for {name}...")
                    detail_html = await self.fetch_page(detail_url)
                    detail_soup = BeautifulSoup(detail_html, "html.parser")
                    
                    twitter, telegram, website = None, None, None
                    for link in detail_soup.select(".soc_links a"):
                        href = link.get("href", "")
                        if "twitter.com" in href or "x.com" in href: twitter = href
                        elif "t.me" in href or "telegram" in href: telegram = href
                        elif "website" in link.get_text(strip=True).lower(): website = href
                            
                    leads.append(RawLead(
                        name=name,
                        source="icodrops",
                        website=website,
                        twitter_handle=twitter,
                        extra_data={
                            "telegram_channel": telegram,
                            "launch_date": "Upcoming",
                            "description": "Scraped from ICO Drops"
                        }
                    ))
                    await asyncio.sleep(1)
                except Exception as e:
                    self.logger.error(f"Error parsing ICO Drop: {e}")
        except Exception as e:
            self.logger.error(f"ICO Drops Error: {e}")

        # 2. CryptoRank (Simple fallback or specific scraper)
        # Note: CryptoRank is heavy JS. Validating if we can just use requests or need Playwright.
        # For now, we will add a placeholder or simple attempt.
        try:
            self.logger.info("Scraping CryptoRank (Upcoming)...")
            # Using API if possible is better, but site scraping:
            # They use Next.js.
            pass 
        except Exception:
            pass
            
        return leads
