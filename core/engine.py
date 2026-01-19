import asyncio
import time
import uuid
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import desc
from storage.database import SessionLocal
from storage.models import Lead, LeadSource, RunLog
from collectors.defillama import DeFiLlamaCollector
from collectors.cryptorank import CryptoRankCollector
from collectors.x_keywords import XKeywordCollector
from enrichment.pipeline import EnrichmentPipeline
from core.logger import app_logger
import urllib.parse

class StratosphereEngine:
    def __init__(self):
        self.logger = app_logger
        self.stop_requested = False
        # Initialize with Idle state
        self.state = {
            "state": "idle",
            "run_id": "",
            "started_at": None,
            "updated_at": datetime.utcnow().isoformat(),
            "completed_at": None,
            "discovered": 0,
            "enriched": 0,
            "ready_to_dm": 0,
            "needs_ai": 0,
            "current_step": "Ready",
            "current_target": "",
            "progress": 0
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
            if k in self.state:
                self.state[k] = v
                
        self.state["updated_at"] = datetime.utcnow().isoformat()

    async def run(self):
        self.stop_requested = False
        run_id = str(uuid.uuid4())[:8]
        self.state = {
            "state": "running",
            "run_id": run_id,
            "started_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "completed_at": None,
            "discovered": 0,
            "enriched": 0,
            "ready_to_dm": 0,
            "needs_ai": 0,
            "current_step": "Initializing",
            "current_target": "",
            "progress": 0
        }
        
        self.logger.info(f"🚀 Engine Started (Run {run_id})")
        
        try:
            await asyncio.wait_for(self._run_logic(), timeout=300) # 5 min global max
            self.update_state("done", step="Complete", progress=100)
            
        except asyncio.TimeoutError:
            self.logger.error("Global Timeout Exceeded")
            self.update_state("error", step="Timed Out")
        except Exception as e:
             self.logger.error(f"Run crashed: {e}")
             self.update_state("error", step=f"Error: {str(e)}")
        finally:
             self.state["completed_at"] = datetime.utcnow().isoformat()
             # If stopped manually
             if self.stop_requested:
                 self.update_state("done", step="Stopped by user")

    async def _run_logic(self):
        db = SessionLocal()
        try:
            # 1. Collection
            self.update_state(step="Collecting", progress=10)
            collectors = [
                DeFiLlamaCollector(),
                CryptoRankCollector(),
                XKeywordCollector(),
            ]
            
            raw_leads = []
            for c in collectors:
                if self.stop_requested: break
                self.update_state(target=c.name)
                leads = await c.run()
                raw_leads.extend(leads)
                self.update_state(discovered=len(raw_leads))
                
            self.update_state(discovered=len(raw_leads))
                
            if self.stop_requested: return

            self.update_state(step="Ingesting", progress=30)
            
            # 2. Ingestion & Dedup
            new_count = 0
            seen_domains = set()
            seen_handles = set()
            
            for raw in raw_leads:
                try:
                    # Normalization
                    norm_domain = None
                    norm_handle = None
                    if raw.website:
                        parsed = urllib.parse.urlparse(raw.website)
                        if not parsed.netloc: parsed = urllib.parse.urlparse("https://" + raw.website)
                        norm_domain = parsed.netloc.replace("www.", "").lower()
                    if raw.twitter_handle:
                        norm_handle = raw.twitter_handle.lower().replace("@", "").strip()
                        if "twitter.com/" in norm_handle: norm_handle = norm_handle.split("/")[-1]
                    
                    # 1. In-Batch Dedup
                    if norm_domain and norm_domain in seen_domains: continue
                    if norm_handle and norm_handle in seen_handles: continue
                    
                    if norm_domain: seen_domains.add(norm_domain)
                    if norm_handle: seen_handles.add(norm_handle)

                    # 2. Database Dedup (Check if exists)
                    exists = False
                    if norm_domain and db.query(Lead).filter(Lead.normalized_domain == norm_domain).first(): exists = True
                    if not exists and norm_handle and db.query(Lead).filter(Lead.normalized_handle == norm_handle).first(): exists = True
                            
                    if exists: continue
                        
                    lead = Lead(
                        project_name=raw.name,
                        domain=raw.website,
                        normalized_domain=norm_domain,
                        twitter_handle=raw.twitter_handle,
                        normalized_handle=norm_handle,
                        status="New",
                        description=str(raw.extra_data),
                        source_counts=1
                    )
                    if raw.extra_data and "funding" in raw.extra_data: lead.funding_info = raw.extra_data["funding"]
                    
                    source = LeadSource(source_name=raw.source, source_url=raw.website)
                    lead.sources.append(source)
                    db.add(lead)
                    new_count += 1
                    
                except Exception as e:
                    self.logger.error(f"Ingestion failed for {raw.name if raw else 'Unknown'}: {e}")
            
            try:
                db.commit()
            except Exception as e:
                self.logger.error(f"Commit failed: {e}")
                db.rollback()
            
            # 3. Enrichment (Concurrent)
            self.update_state(step="Enriching", progress=40)
            
            leads_to_process = db.query(Lead).filter(Lead.status == "New").all()
            total_enrich = len(leads_to_process)
            
            # Semaphore for concurrency
            sem = asyncio.Semaphore(5)
            
            async def safe_process(lead):
                if self.stop_requested: return
                async with sem:
                    try:
                        self.update_state(target=lead.project_name)
                        pipeline = EnrichmentPipeline(db) # Create pipeline inside task if needed or pass db
                        # Note: Pipeline init might be safer outside if db is thread-safe, 
                        # but here we use the same db session. Carefully.
                        # Ideally pipeline uses its own session or we pass the main one.
                        # Re-instantiating pipeline is cheap.
                        p = EnrichmentPipeline(db)
                        await p.process_lead(lead)
                        
                        self.state["enriched"] += 1
                        pct = 40 + int((self.state["enriched"] / max(1, total_enrich)) * 50)
                        self.update_state(progress=pct)
                        
                        # Update Qualified Counts
                        if lead.bucket == "READY_TO_DM": self.state["ready_to_dm"] += 1
                        elif lead.bucket == "NEEDS_ALT_OUTREACH": self.state["needs_ai"] += 1
                        
                        # Periodic commit
                        if self.state["enriched"] % 5 == 0: db.commit()
                            
                    except Exception as e:
                        self.logger.error(f"Enrich failed for {lead.project_name}: {e}")
            
            tasks = [safe_process(l) for l in leads_to_process]
            if tasks:
                await asyncio.gather(*tasks)
                
            db.commit() # Final commit
            self.log_run(db, "Pipeline", "INFO", f"Run {self.state['run_id']} Complete.")
            
        except Exception as e:
            self.logger.error(f"Engine Failed: {e}")
            self.log_run(db, "Engine", "ERROR", str(e))
            raise e
        finally:
            db.close()

    def log_run(self, db: Session, component: str, level: str, message: str):
        try:
            log = RunLog(component=component, level=level, message=message)
            db.add(log)
            db.commit()
        except:
            pass

engine_instance = StratosphereEngine()
