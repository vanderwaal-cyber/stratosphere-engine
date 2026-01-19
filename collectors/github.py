import asyncio
import random
from typing import List
from bs4 import BeautifulSoup
import urllib.parse
from collectors.base import BaseCollector, RawLead
from datetime import datetime, timedelta

class GithubCollector(BaseCollector):
    def __init__(self):
        super().__init__("github_repos")
        self.queries = [
            "topic:solidty", 
            "topic:web3-dapp", 
            "readme:defi protocol", 
            "readme:tokenomics", 
            "topic:smart-contracts"
        ]

    async def collect(self) -> List[RawLead]:
        leads = []
        try:
            # We want recently updated/created repos
            # GitHub Search Syntax: created:>YYYY-MM-DD
            yesterday = (datetime.utcnow() - timedelta(days=2)).strftime('%Y-%m-%d')
            
            chosen_queries = random.sample(self.queries, 2)
            for q in chosen_queries:
                full_query = f"{q} created:>{yesterday}"
                encoded_q = urllib.parse.quote(full_query)
                url = f"https://github.com/search?q={encoded_q}&type=repositories&s=updated&o=desc"
                
                self.logger.info(f"🐙 GitHub Search: '{full_query}'")
                await asyncio.sleep(random.uniform(2.0, 5.0))
                
                html = await self.fetch_page(url)
                if not html: continue
                
                soup = BeautifulSoup(html, 'html.parser')
                # GitHub class names change often, but data-testid are usually stable
                # Or standard search result structures. 
                # Fallback to loose scraping if classes fail.
                
                repo_list = soup.find_all('div', {'class': 'search-title'})
                if not repo_list:
                    # Try modern GitHub search structure (react-like)
                    # It's harder to scrape raw HTML from GitHub as it's often SPA now.
                    # We might need to rely on the JSON API if HTML fails, but let's try a simpler public page
                    # Or use a 'topic' page which is SSR.
                    pass

                # Strategy B: Scrape Topic Pages (More reliable HTML)
                # https://github.com/topics/defi?o=desc&s=updated
                
            # SWITCHING STRATEGY TO TOPICS (Better SSR)
            topics = ["defi", "web3", "smart-conrtacts", "solana", "ethereum"]
            chosen_topics = random.sample(topics, 3)
            
            for topic in chosen_topics:
                url = f"https://github.com/topics/{topic}?o=desc&s=updated"
                self.logger.info(f"🐙 GitHub Topic: '{topic}'")
                await asyncio.sleep(random.uniform(1.0, 3.0))
                
                html = await self.fetch_page(url)
                soup = BeautifulSoup(html, 'html.parser')
                
                articles = soup.find_all('article', class_='border rounded-2 box-shadow-bg-gray-light my-4')
                
                for art in articles:
                    h3 = art.find('h3')
                    if not h3: continue
                    links = h3.find_all('a')
                    if len(links) < 2: continue
                    
                    user = links[0].get_text(strip=True)
                    repo = links[1].get_text(strip=True)
                    relative_url = links[1].get('href')
                    
                    full_link = f"https://github.com{relative_url}"
                    
                    # Description
                    desc_div = art.find('div', class_='color-text-secondary')
                    desc = desc_div.get_text(strip=True) if desc_div else "New Repo"
                    
                    leads.append(RawLead(
                        name=f"{repo} ({user})",
                        source="github_topic",
                        website=full_link,
                        extra_data={
                            "desc": desc,
                            "topic": topic
                        }
                    ))
                    
            self.logger.info(f"✅ GitHub found {len(leads)} repos")
                
        except Exception as e:
            self.logger.error(f"GitHub Collector failed: {e}")
            
        return leads
