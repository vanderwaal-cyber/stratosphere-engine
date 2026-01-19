import asyncio
import time
import uuid
import random
from datetime import datetime
from sqlalchemy.orm import Session
from storage.database import SessionLocal
from storage.models import Lead, LeadSource, RunLog
from collectors.defillama import DeFiLlamaCollector
from collectors.x_keywords import XKeywordCollector
from collectors.search import UniversalSearchCollector
from collectors.github import GithubCollector
from collectors.launchpads import LaunchpadCollector
from core.logger import app_logger
import urllib.parse

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
            "stats": {"new_added": 0, "duplicates_skipped": 0, "failed_ingestion": 0, "total_scraped": 0, "loops": 0}
        }
        
        self.logger.info(f"🚀 Engine Started (Run {run_id}) | Mode: {mode}")
        
        try:
            # 5 Minute Global Timeout
            await asyncio.wait_for(self._run_collection_phase(mode, run_id), timeout=300)
            self.update_state("done", step="Complete", progress=100)
            
        except asyncio.TimeoutError:
            self.logger.warning("Global Timeout Exceeded. Stopping gracefully.")
            self.update_state("done", step="Timed Out (Partial Results)")
        except Exception as e:
             self.logger.error(f"Run crashed: {e}")
             self.update_state("error", step=f"Error: {str(e)}")
        finally:
             self.state["completed_at"] = datetime.utcnow().isoformat()
             if self.stop_requested: self.update_state("done", step="Stopped by user")

    async def _run_collection_phase(self, mode, run_id):
        db = SessionLocal()
        try:
            # REAL SOURCES ONLY - No Synthetic
            collectors = [
                UniversalSearchCollector(), # The Heavy Lifter
                GithubCollector(),          # Re-enabled with care
                LaunchpadCollector(),
                DeFiLlamaCollector(),
                XKeywordCollector(),
            ]
            
            target_leads = 1000 
            max_loops = 50 
            
            while self.state["stats"]["new_added"] < target_leads and self.state["stats"]["loops"] < max_loops:
                if self.stop_requested: break
                
                self.state["stats"]["loops"] += 1
                loop_idx = self.state["stats"]["loops"]
                
                # Dynamic Progress
                pct = min(95, int((self.state["stats"]["new_added"] / target_leads) * 100))
                self.update_state(step=f"Mining (Loop {loop_idx}) - {self.state['stats']['new_added']} Leads", progress=pct)
                
                found_any_in_loop = False
                
                for c in collectors:
                    if self.stop_requested: break
                    if self.state["stats"]["new_added"] >= target_leads: break
                    
                    try: 
                        self.update_state(target=c.name)
                        leads = await c.run()
                        if leads:
                            found_any_in_loop = True
                            for raw in leads:
                                if self.state["stats"]["new_added"] >= target_leads: break
                                await self._process_lead(db, raw, run_id)
                                
                    except Exception as e:
                        self.logger.error(f"Collector {c.name} failed: {e}")
                        continue
                
                # NO SYNTHETIC FILL. If we run dry, we run dry.
                # But UniversalSearchCollector is infinite by design (random queries), so it shouldn't run dry easily.
                
                if not found_any_in_loop:
                    self.logger.info("Loop yielded 0 results. Pausing briefly to switch IP/Queries.")
                    await asyncio.sleep(2)
                    
        finally:
            db.close()

    async def _process_lead(self, db, raw, run_id):
        # STRICT VERIFICATION: Must have a URL or Handle
        if not raw.website and not raw.twitter_handle:
            return False
            
        try:
            # Normalization
            norm_domain = None
            norm_handle = None
            
            if raw.website:
                # Basic cleanup
                if "http" not in raw.website: raw.website = f"https://{raw.website}"
                try:
                    parsed = urllib.parse.urlparse(raw.website)
                    norm_domain = parsed.netloc.replace("www.", "").lower()
                except: pass
                
            if raw.twitter_handle:
                norm_handle = raw.twitter_handle.lower().replace("@", "").strip()
                if "twitter.com/" in norm_handle: norm_handle = norm_handle.split("/")[-1]
                if "x.com/" in norm_handle: norm_handle = norm_handle.split("/")[-1]
                
            # Deduplication
            existing = None
            if norm_handle:
                existing = db.query(Lead).filter(Lead.normalized_handle == norm_handle).first()
            if not existing and norm_domain:
                existing = db.query(Lead).filter(Lead.normalized_domain == norm_domain).first()

            if existing:
                self.state["stats"]["duplicates_skipped"] += 1
                return False
                
            # Create NEW Verified Lead
            lead = Lead(
                project_name=raw.name[:100],
                domain=raw.website,
                normalized_domain=norm_domain,
                twitter_handle=f"@{norm_handle}" if norm_handle else None,
                normalized_handle=norm_handle,
                status="New",
                description=str(raw.extra_data)[:500],
                source_counts=1,
                created_at=datetime.utcnow(),
                run_id=run_id 
            )
            db.add(lead)
            db.commit()
            
            self.state["stats"]["new_added"] += 1
            self.state["discovered"] += 1
            return True
            
        except Exception as e:
            db.rollback()
            self.state["stats"]["failed_ingestion"] += 1
            return False

    def log_run(self, db, component, level, message):
        try:
            log = RunLog(component=component, level=level, message=message)
            db.add(log)
            db.commit()
        except: pass

engine_instance = StratosphereEngine()
