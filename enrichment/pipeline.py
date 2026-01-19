import asyncio
from typing import Dict, Any
from sqlalchemy.orm import Session

from core.logger import app_logger
from storage.models import Lead
from enrichment.website import WebsiteScraper
from enrichment.social import SocialExtractor
import urllib.parse

from core.logger import app_logger
from storage.models import Lead
from enrichment.website import WebsiteScraper
from enrichment.social import SocialExtractor
from enrichment.search import search_x_handle
import urllib.parse

class EnrichmentPipeline:
    def __init__(self, db: Session):
        self.db = db
        self.scraper = WebsiteScraper()
        self.extractor = SocialExtractor()

    async def process_lead(self, lead: Lead):
        app_logger.info(f"[Enrichment] Processing {lead.project_name}")
        
        # 0. Normalize Domain
        if lead.domain:
            parsed = urllib.parse.urlparse(lead.domain)
            if not parsed.scheme: 
                lead.domain = "https://" + lead.domain
                parsed = urllib.parse.urlparse(lead.domain)
            lead.normalized_domain = parsed.netloc.replace('www.', '')
        
        # Check Dedup (Strict V2)
        if lead.normalized_domain:
            exists = self.db.query(Lead).filter(
                Lead.normalized_domain == lead.normalized_domain,
                Lead.id != lead.id
            ).first()
            if exists:
                lead.status = "Disqualified"
                lead.reject_reason = "Duplicate Domain"
                # Consolidate info if needed? Discard for now.
                return

        # 1. Scrape Website
        if lead.domain:
            html = await self.scraper.fetch_html(lead.domain)
            socials = self.extractor.extract_all(html)
            
            # Update Lead
            if socials['twitter']: lead.twitter_handle = socials['twitter']
            if socials['discord']: lead.discord_url = socials['discord']
            if socials['telegram']: lead.telegram_url = socials['telegram']
            if socials['email']: lead.email = socials['email']
                
        # 2. Fallback Search (Deep Verify)
        if not lead.twitter_handle and lead.project_name:
            app_logger.info(f"  🔍 Searching X handle for {lead.project_name}...")
            found_handle = await search_x_handle(lead.project_name, lead.domain or "")
            if found_handle:
                lead.twitter_handle = found_handle
                app_logger.info(f"  ✅ Found handle via search: @{found_handle}")

        # 3. Normalize Handle
        if lead.twitter_handle:
            lead.normalized_handle = lead.twitter_handle.lower()
            # Dedup Handle
            exists = self.db.query(Lead).filter(
                Lead.normalized_handle == lead.normalized_handle,
                Lead.id != lead.id
            ).first()
            if exists:
                lead.status = "Disqualified"
                lead.reject_reason = "Duplicate Handle"
                return

        # 4. Strict Scoring & Bucketing (V2)
        self.score_lead_v2(lead)
        
    def score_lead_v2(self, lead: Lead):
        score = 0
        reasons = []
        
        # Must-haves for "Ready to DM"
        has_site = bool(lead.domain)
        has_x = bool(lead.twitter_handle)
        
        if has_site: score += 30
        if has_x: score += 40
        
        # Telemetry
        if lead.telegram_url: score += 20
        if lead.email: score += 10
        
        lead.score = min(score, 100)
        
        # Rules
        # Ready to DM: Website + X + (TG or Email)
        # Actually user said: "X DM if open OR email OR TG admin".
        # We can't verify "DM Open" easily without auth, so we assume X is contactable if present.
        
        if has_site and has_x:
            lead.bucket = "READY_TO_DM"
            lead.status = "Qualified"
        elif has_site:
            lead.bucket = "NEEDS_ALT_OUTREACH" # Missing X, but has site (maybe email/tg)
            lead.status = "Qualified"
        else:
            lead.bucket = "MANUAL_CHECK"
            lead.status = "Enriched"
            
        if not has_x:
            reasons.append("Missing X")
            
        if reasons:
            lead.reject_reason = ", ".join(reasons)
