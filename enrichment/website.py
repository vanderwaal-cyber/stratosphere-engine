import httpx
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from core.logger import app_logger

class WebsiteScraper:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }

    async def fetch_html(self, url: str) -> str:
        """
        Tries HTTPX first. If it fails or looks sparse, uses Playwright.
        """
        # 1. Try Fast HTTPX
        try:
            async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
                response = await client.get(url, headers=self.headers)
                if response.status_code == 200:
                    html = response.text
                    # Check if JS required (simple check)
                    if "<noscript>" in html or len(html) < 500:
                        app_logger.info(f"[Scraper] {url} looks like SPA, switching to Playwright.")
                        return await self.fetch_with_playwright(url)
                    return html
        except Exception as e:
            app_logger.warning(f"[Scraper] HTTPX failed for {url}: {e}. Switching to Playwright.")
            
        # 2. Playwright Fallback
        return await self.fetch_with_playwright(url)

    async def fetch_with_playwright(self, url: str) -> str:
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                await page.goto(url, wait_until="domcontentloaded", timeout=15000)
                content = await page.content()
                await browser.close()
                return content
        except Exception as e:
            app_logger.error(f"[Scraper] Playwright failed for {url}: {e}")
            return ""
