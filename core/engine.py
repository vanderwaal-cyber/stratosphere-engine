import asyncio
import random
import time
import uuid
import urllib.parse
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from storage.database import SessionLocal
from storage.models import Lead, LeadSource, RunLog
from collectors.x_keywords import XKeywordCollector
from collectors.x_api import XApiCollector
from collectors.defillama import DeFiLlamaCollector
from collectors.search import UniversalSearchCollector
from collectors.github import GithubCollector
from collectors.launchpads import LaunchpadCollector
from collectors.coinmarketcap import CoinMarketCapCollector
from collectors.ico_calendars import ICOCalendarCollector
from collectors.coingecko import CoinGeckoCollector # User fallback
from core.logger import app_logger
from core.notifications import NotificationManager

class StratosphereEngine:
    def __init__(self):
        self.logger = app_logger
        self.stop_requested = False
        self.state = {
            "state": "idle",
            "run_id": "",
            "started_at": None,
            "updated_at": datetime.utcnow().isoformat(),
            "completed_at": None,
            "discovered": 0,
            "progress": 0,
            "current_step": "Ready",
            "stats": {
                "new_added": 0,
                "duplicates_skipped": 0,
                "merged_updates": 0,
                "failed_ingestion": 0,
                "total_scraped": 0,
                "loops": 0
            }
        }
    
    def stop(self):
        self.stop_requested = True
        self.update_state("stopping", step="Stopping...")
        
    def update_state(self, status=None, step=None, target=None, progress=None, **kwargs):
        if status: self.state["state"] = status
        if step: self.state["current_step"] = step
        if target: self.state["current_target"] = target
        if progress is not None: self.state["progress"] = progress
        for k, v in kwargs.items():
            if k in self.state: self.state[k] = v
        self.state["updated_at"] = datetime.utcnow().isoformat()

    async def run(self, mode="fresh", run_id=None):
        self.stop_requested = False
        if not run_id: run_id = str(uuid.uuid4())[:8]
            
        self.state = {
            "state": "running",
            "run_id": run_id,
            "started_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "completed_at": None,
            "discovered": 0,
            "progress": 0,
            "current_step": "Initializing",
            "stats": {"new_added": 0, "duplicates_skipped": 0, "merged_updates": 0, "failed_ingestion": 0, "total_scraped": 0, "loops": 0}
        }
        
        self.logger.info(f"🚀 Engine Started (Run {run_id}) | Mode: {mode}")
        
        try:
            # 10 Minute Global Timeout (increased for heavy scrape)
            await asyncio.wait_for(self._run_collection_phase(mode, run_id), timeout=600)
            self.update_state("done", step="Complete", progress=100)
            
            # NOTIFICATION
            try:
                notifier = NotificationManager()
                await notifier.notify_run_completion(self.state["stats"]["new_added"], "Auto-Detected Source")
            except Exception as ne:
                self.logger.error(f"Notification Failed: {ne}")

        except asyncio.TimeoutError:
            self.logger.warning("Global Timeout Exceeded. Stopping gracefully.")
            self.update_state("done", step="Timed Out (Partial Results)")
        except Exception as e:
             self.logger.error(f"Run crashed: {e}", exc_info=True)
             self.update_state("error", step=f"Error: {str(e)}")
        finally:
             self.state["completed_at"] = datetime.utcnow().isoformat()
             if self.stop_requested: self.update_state("done", step="Stopped by user")

    async def _run_collection_phase(self, mode, run_id):
        db = SessionLocal()
        try:
            # PRIORITY ORDER
            collectors = [
                CoinMarketCapCollector(),  # PRIMARY VOLUME
                ICOCalendarCollector(),    # UPCOMING LAUNCHES
                XApiCollector(),           # LIVE SOCIALS
                DeFiLlamaCollector(),      # QUALITY DeFi
                CoinGeckoCollector(),      # FALLBACK VOLUME
                # UniversalSearchCollector(), # Search - Supplemental
            ]
            
            target_leads = 200 # User requested 200+ daily
            
            # Start Loop
            for c in collectors:
                if self.stop_requested: break
                if self.state["stats"]["new_added"] >= target_leads: 
                     self.logger.info("Target leads reached. Stopping collection.")
                     break
                
                try: 
                    self.update_state(step=f"Running {c.name}...")
                    leads = await c.run(self.update_state) 
                    
                    found_count = len(leads)
                    self.state["stats"]["total_scraped"] += found_count
                    
                    if found_count > 0:
                        for raw in leads:
                            if self.stop_requested: break
                            await self._process_lead(db, raw, run_id)
                    else:
                        self.logger.info(f"{c.name} yielded 0 results.")
                            
                except Exception as e:
                    self.logger.error(f"Collector {c.name} failed: {e}")
                    continue
                
                await asyncio.sleep(1)

        finally:
            db.close()

    async def _process_lead(self, db, raw, run_id):
        # STRICT VERIFICATION: Must have a Name
        if not raw.name: return False
            
        try:
            # Normalization
            norm_domain = None
            norm_handle = None
            norm_telegram = None
            
            if raw.website:
                if "http" not in raw.website: raw.website = f"https://{raw.website}"
                try:
                    parsed = urllib.parse.urlparse(raw.website)
                    norm_domain = parsed.netloc.replace("www.", "").lower()
                except: pass
                
            if raw.twitter_handle:
                norm_handle = raw.twitter_handle.lower().replace("@", "").strip()
                if "twitter.com/" in norm_handle: norm_handle = norm_handle.split("/")[-1]
                if "x.com/" in norm_handle: norm_handle = norm_handle.split("/")[-1]
                # Clean query params
                if "?" in norm_handle: norm_handle = norm_handle.split("?")[0]
            
            # Get Telegram from extra_data or other fields
            telegram = raw.extra_data.get("telegram_channel")
            if telegram:
                 # Normalize: t.me/username -> username
                 norm_telegram = telegram.replace("https://", "").replace("http://", "").replace("t.me/", "").replace("telegram.me/", "").strip()
                 if "/" in norm_telegram: norm_telegram = norm_telegram.split("/")[0] # handle t.me/user/extra
                 # Remove @ if present
                 norm_telegram = norm_telegram.replace("@", "")

            # Deduplication Strategy:
            # 1. Match Telegram (Strongest Signal)
            # 2. Match Twitter
            # 3. Match Domain
            
            existing = None
            
            if norm_telegram:
                existing = db.query(Lead).filter(Lead.telegram_channel == norm_telegram).first()
            
            if not existing and norm_handle:
                existing = db.query(Lead).filter(Lead.normalized_handle == norm_handle).first()
                
            if not existing and norm_domain:
                existing = db.query(Lead).filter(Lead.normalized_domain == norm_domain).first()

            # Prepare data
            chains_data = raw.extra_data.get("chains", [])
            tags_data = raw.extra_data.get("tags", [])
            # Convert to strings for DB
            import json
            chains_str = json.dumps(chains_data) if chains_data else None
            tags_str = json.dumps(tags_data) if tags_data else None
            launch_date = raw.extra_data.get("launch_date")

            if existing:
                # DEDUPLICATION: Strict Mode
                # User Request: "Ignore if twitter already seen"
                # If we matched based on a unique identifier (Handle or Telegram), it is a duplicate.
                # We will NOT bump it or merge it to avoid cluttering specific run stats.
                
                self.state["stats"]["duplicates_skipped"] += 1
                # Optional: Only update missing fields silently?
                # For now, completely skip to satisfy "Don't add repeats" requirement visual.
                return False
                
            # Create NEW Verified Lead
            description = raw.extra_data.get("description") or f"Discovered on {raw.source}"
            
            # QUALITY FILTER (Anti-Spam)
            # If a lead has NO Twitter AND NO Website, it is considered "bland"/useless.
            if not norm_handle and not raw.website:
                # Exception: unless it has a strong Telegram signal
                if not norm_telegram:
                     self.logger.info(f"Skipping Low Quality Lead (No Socials): {raw.name}")
                     return False
            
            # SCORING SYSTEM (Quality Check)
            score = 0
            if norm_telegram: score += 30 # Strongest Signal
            if norm_handle: score += 10
            if raw.website: score += 10
            
            # Freshness Bonus
            is_upcoming = False
            if launch_date:
                try:
                    ld = launch_date
                    if isinstance(ld, str):
                        # Try parsing various formats if needed, or assume ISO
                         ld = datetime.fromisoformat(ld.replace("Z", "+00:00"))
                    
                    if ld > datetime.utcnow():
                        score += 10
                        is_upcoming = True
                    elif (datetime.utcnow() - ld).days < 7:
                        score += 10 # Recent launch
                except: path
            
            # Bucketing Logic
            bucket = None
            if score >= 40:
                bucket = "READY_TO_DM"
            elif is_upcoming:
                 bucket = "UPCOMING_WATCHLIST"
            elif norm_handle:
                 bucket = "NEEDS_ENRICHMENT"

            lead = Lead(
                project_name=raw.name[:100],
                source=raw.source,
                domain=raw.website,
                normalized_domain=norm_domain,
                twitter_handle=f"@{norm_handle}" if norm_handle else None,
                normalized_handle=norm_handle,
                telegram_channel=norm_telegram,
                telegram_url=telegram,
                chains=chains_str,
                tags=tags_str,
                launch_date=launch_date,
                profile_image_url=raw.profile_image_url 
                or (norm_handle and f"https://unavatar.io/twitter/{norm_handle}")
                or (norm_domain and f"https://logo.clearbit.com/{norm_domain}")
                or f"https://ui-avatars.com/api/?name={urllib.parse.quote(raw.name)}&background=random&color=fff",
                status="New",
                description=str(description)[:500],
                score=score,
                bucket=bucket,
                source_counts=1,
                created_at=datetime.utcnow(),
                run_id=run_id 
            )
            db.add(lead)
            db.flush() # get ID
            
            # Add Source
            source_entry = LeadSource(
                lead_id=lead.id,
                source_name=raw.source,
                source_url=raw.website
            )
            db.add(source_entry)
            
            db.commit()
            db.refresh(lead)

            self.state["stats"]["new_added"] += 1
            self.state["discovered"] += 1
            return True
            
        except Exception as e:
            db.rollback()
            self.state["stats"]["failed_ingestion"] += 1
            # self.logger.error(f"Ingestion error: {e}")
            return False

engine_instance = StratosphereEngine()
