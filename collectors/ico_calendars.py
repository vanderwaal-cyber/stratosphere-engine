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
        
        for source in self.sources:
            try:
                self.logger.info(f"Scraping {source['name']} ({source['url']})...")
                html = await self.fetch_page(source['url'])
                soup = BeautifulSoup(html, "html.parser")
                
                # ICO Drops structure: div.a_ico
                ico_cards = soup.select("div.a_ico")
                self.logger.info(f"Found {len(ico_cards)} cards on {source['name']}")
                
                for card in ico_cards:
                    try:
                        name_tag = card.select_one("h3 a")
                        if not name_tag:
                            continue
                            
                        name = name_tag.get_text(strip=True)
                        detail_url = name_tag['href']
                        
                        # Fetch detail page to get socials?
                        # This works best if we do it lazily or batch, but simpler to just do standard request for high value leads.
                        # Ideally we don't spam 100 requests.
                        # Let's extract what we can from listing first.
                        # Listing has categories usually.
                        
                        categories = []
                        cat_tag = card.select_one(".ico-category-name")
                        if cat_tag:
                            categories.append(cat_tag.get_text(strip=True))
                        
                        # We need socials. ICODrops detail page has them.
                        # Let's just create the lead with the Name & Source.
                        # The "Enrichment" step (not implemented fully yet but planned) would normally fill the gaps.
                        # BUT user wants Telegram PRIORITY.
                        # So we MUST fetch details for these significantly valuable leads.
                        # We will fetch details for the first 10-20 to avoid timeouts/bans, then rely on enrichment?
                        # Or sleep.
                        
                        self.logger.info(f"Fetching details for {name}...")
                        detail_html = await self.fetch_page(detail_url)
                        detail_soup = BeautifulSoup(detail_html, "html.parser")
                        
                        # Socials
                        twitter = None
                        telegram = None
                        website = None
                        
                        # Social list usually in .soc_links
                        social_links = detail_soup.select(".soc_links a")
                        for link in social_links:
                            href = link.get("href", "")
                            if "twitter.com" in href or "x.com" in href:
                                twitter = href
                            elif "t.me" in href or "telegram" in href:
                                telegram = href
                            elif "website" in link.get_text(strip=True).lower() or link.find("i", class_="fa-globe"):
                                website = href
                        
                        # Website often in plain button too "WEBSITE"
                        if not website:
                            web_btn = detail_soup.find("a", string=re.compile("WEBSITE", re.IGNORECASE))
                            if web_btn:
                                website = web_btn.get("href")

                        lead = RawLead(
                            name=name,
                            source=f"icodrops_{source['name']}",
                            website=website,
                            twitter_handle=twitter,
                            extra_data={
                                "telegram_channel": telegram,
                                "tags": categories,
                                "description": f"Scraped from {source['name']}",
                                "launch_date": "Upcoming" # Parse date if possible
                            }
                        )
                        leads.append(lead)
                        await asyncio.sleep(1) # Polite delay
                        
                    except Exception as e:
                        self.logger.error(f"Error parsing card for {name_tag}: {e}")
                        continue
                        
            except Exception as e:
                self.logger.error(f"Error scraping {source['name']}: {e}")
                
        return leads
