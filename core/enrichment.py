import re
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

class EnrichmentEngine:
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }

    async def enrich_url(self, url: str) -> dict:
        """
        Visits a URL and extracts Emails, Telegram, and Discord links.
        Returns a dict of found data.
        """
        if not url or "http" not in url:
            return {}

        print(f"🔎 Enriching: {url}...")
        try:
            async with aiohttp.ClientSession(headers=self.headers) as session:
                async with session.get(url, timeout=10) as response:
                    if response.status != 200:
                        return {}
                    html = await response.text()
                    return self._parse_html(html, url)
        except Exception as e:
            print(f"Enrichment Failed for {url}: {e}")
            return {}

    def _parse_html(self, html: str, base_url: str) -> dict:
        soup = BeautifulSoup(html, 'html.parser')
        data = {
            "email": None,
            "telegram_url": None,
            "discord_url": None,
            "twitter_handle": None
        }

        # 1. Extract Links
        for a in soup.find_all('a', href=True):
            href = a['href']
            full_url = urljoin(base_url, href)
            
            # Email
            if "mailto:" in href and not data["email"]:
                data["email"] = href.replace("mailto:", "").split("?")[0].strip()

            # Telegram
            if "t.me/" in href or "telegram.me/" in href:
                if "joinchat" not in href: # Prefer main channels
                    data["telegram_url"] = full_url

            # Discord
            if "discord.gg/" in href or "discord.com/invite/" in href:
                data["discord_url"] = full_url

            # Twitter (Validation or Enhancement)
            if "twitter.com/" in href or "x.com/" in href:
                if "/status/" not in href and "intent" not in href:
                    handle = href.split("/")[-1].split("?")[0]
                    if handle and not data["twitter_handle"]:
                        data["twitter_handle"] = handle

        # 2. Text Search (Backup for Emails)
        if not data["email"]:
            # Simple regex for emails
            emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', soup.get_text())
            if emails:
                # Filter out garbage emails (e.g. image@x.png)
                valid_emails = [e for e in emails if not e.endswith(('.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp'))]
                if valid_emails:
                    data["email"] = valid_emails[0]

        return {k: v for k, v in data.items() if v} # Compact result

# Usage Example:
# engine = EnrichmentEngine()
# data = await engine.enrich_url("https://example.com")
