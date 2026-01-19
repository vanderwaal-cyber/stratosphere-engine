import abc
import time
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import httpx

from core.logger import app_logger
from core.config import get_settings

settings = get_settings()

@dataclass
class RawLead:
    name: str
    source: str
    website: Optional[str] = None
    twitter_handle: Optional[str] = None
    extra_data: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self):
        return {
            "name": self.name,
            "source": self.source,
            "website": self.website,
            "twitter_handle": self.twitter_handle,
            "extra_data": self.extra_data
        }

class BaseCollector(abc.ABC):
    """
    Abstract Base Class for all collectors.
    Handles user-agent rotation, retries, and error boundaries.
    """
    def __init__(self, name: str):
        self.name = name
        self.logger = app_logger
        self.settings = settings
        self.user_agents = [
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15'
        ]

    def get_headers(self):
        import random
        return {
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5'
        }

    @retry(
        stop=stop_after_attempt(3), 
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(httpx.RequestError)
    )
    async def fetch_page(self, url: str) -> str:
        """
        Fetches a page with retries and timeout.
        """
        async with httpx.AsyncClient(timeout=self.settings.COLLECTOR_TIMEOUT_SECONDS) as client:
            response = await client.get(url, headers=self.get_headers())
            response.raise_for_status()
            return response.text

    async def run(self) -> List[RawLead]:
        """
        Public run wrapper with error boundary.
        """
        self.logger.info(f"[{self.name}] Starting collection...")
        start_time = time.time()
        leads = []
        try:
            leads = await self.collect()
            elapsed = time.time() - start_time
            self.logger.info(f"[{self.name}] Completed in {elapsed:.2f}s. Collected {len(leads)} leads.")
        except Exception as e:
            self.logger.error(f"[{self.name}] CRITICAL FAILURE: {e}", exc_info=True)
            # Return empty list on failure to not crash the whole system
            return []
        return leads

    @abc.abstractmethod
    async def collect(self) -> List[RawLead]:
        """
        Implementation specific logic.
        Must return list of RawLeads.
        """
        pass
