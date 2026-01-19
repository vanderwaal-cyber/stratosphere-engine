import urllib.parse
from bs4 import BeautifulSoup
from collectors.base import BaseCollector
import re

async def search_x_handle(project_name: str, domain: str = "") -> str:
    """
    Falls back to DuckDuckGo search to find X handle.
    Query: "{project_name} {domain} twitter"
    """
    try:
        q = f"{project_name} {domain} twitter"
        encoded = urllib.parse.quote(q)
        url = f"https://html.duckduckgo.com/html/?q={encoded}&kl=us-en"
        
        # We reuse the logic from BaseCollector, but this is a helper.
        # We need a quick fetch. Instantiating a collector just for one fetch is heavy.
        # We'll use a simple httpx call here or refactor.
        # For cleanliness, let's use a specialized Searcher class or just httpx.
        import httpx
        headers = {'User-Agent': 'Mozilla/5.0 ...'}
        
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, headers=headers)
            html = resp.text
            
        soup = BeautifulSoup(html, 'html.parser')
        results = soup.find_all('a', class_='result__a')
        
        for a in results:
            href = a.get('href', '')
            if "twitter.com" in href or "x.com" in href:
                # Extract
                m = re.search(r'(?:twitter\.com|x\.com)/([a-zA-Z0-9_]+)', href)
                if m:
                    handle = m.group(1)
                    if handle.lower() not in ['home', 'explore', 'search']:
                        return handle
                        
    except Exception:
        pass
    return None
