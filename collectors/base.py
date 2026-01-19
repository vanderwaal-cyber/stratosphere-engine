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
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
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
            response = await client.get(url, headers=self.headers)
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
