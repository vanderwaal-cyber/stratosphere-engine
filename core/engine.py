import asyncio
import random
import time
import uuid
import urllib.parse
from datetime import datetime
from sqlalchemy.orm import Session
from storage.database import SessionLocal
from storage.models import Lead, LeadSource, RunLog
from collectors.x_keywords import XKeywordCollector
from collectors.defillama import DeFiLlamaCollector
from collectors.search import UniversalSearchCollector
from collectors.github import GithubCollector
from collectors.launchpads import LaunchpadCollector
from core.logger import app_logger

# Batching & Lead Generation Logic
class LeadBatchGenerator:
    def __init__(self, batch_size=50):
        self.batch_size = batch_size
        self.max_consecutive_duplicates = 10
        self.search_rotations = [
            {"keywords": ["crypto", "defi", "web3", "nft", "blockchain"], "locations": ["", "US", "UK", "Singapore", "Germany"]},
            {"keywords": ["ai", "builder", "founder", "venture"], "locations": ["US", "India", "Canada"]}
        ]
        self._reset_rotation_index()

    def _reset_rotation_index(self):
        self.rotation_index = 0
        self.keyword_index = 0
        self.location_index = 0

    def rotate_search(self):
        self.keyword_index = (self.keyword_index + 1) % len(self.search_rotations[self.rotation_index]['keywords'])
        if self.keyword_index == 0:
            self.location_index = (self.location_index + 1) % len(self.search_rotations[self.rotation_index]['locations'])
            if self.location_index == 0:
                self.rotation_index = (self.rotation_index + 1) % len(self.search_rotations)

    def current_search_params(self):
        sr = self.search_rotations[self.rotation_index]
        kw = sr['keywords'][self.keyword_index]
        loc = sr['locations'][self.location_index]
        return kw, loc

    def generate_unique_leads(self):
        new_leads = []
        found_count = 0
        duplicate_streak = 0
        db: Session = SessionLocal()
        loop = asyncio.new_event_loop()
        try:
            collector = XKeywordCollector()
            asyncio.set_event_loop(loop)
            while found_count < self.batch_size:
                keyword, location = self.current_search_params()
                candidates = loop.run_until_complete(collector.collect_profiles(keyword=keyword, location=location))
                round_added = 0
                for c in candidates:
                    get_val = c.get if isinstance(c, dict) else lambda k: getattr(c, k, None)

                    project_name = get_val("name") or get_val("username") or "Unknown"
                    norm_handle = None
                    username = get_val("username") or get_val("twitter_handle")
                    if username:
                        norm_handle = username.lower().replace("@", "").strip()

                    norm_domain = None
                    raw_url = get_val("url") or get_val("website")
                    if raw_url:
                        try:
                            parsed = urllib.parse.urlparse(raw_url if raw_url.startswith("http") else f"https://{raw_url}")
                            norm_domain = parsed.netloc.replace("www.", "").lower()
                        except Exception:
                            norm_domain = None

                    existing = None
                    if norm_handle:
                        existing = db.query(Lead).filter(Lead.normalized_handle == norm_handle).first()
                    if not existing and norm_domain:
                        existing = db.query(Lead).filter(Lead.normalized_domain == norm_domain).first()

                    if existing:
                        duplicate_streak += 1
                        if duplicate_streak > self.max_consecutive_duplicates:
                            self.rotate_search()
                            duplicate_streak = 0
                        continue

                    lead = Lead(
                        project_name=project_name[:100],
                        domain=raw_url,
                        normalized_domain=norm_domain,
                        twitter_handle=f"@{norm_handle}" if norm_handle else None,
                        normalized_handle=norm_handle,
                        profile_image_url=get_val("profile_image_url")
                        or (norm_handle and f"https://unavatar.io/twitter/{norm_handle}")
                        or f"https://ui-avatars.com/api/?name={urllib.parse.quote(project_name)}&background=random&color=fff",
                        status="New",
                        description=str(c)[:500],
                        source_counts=1,
                        created_at=datetime.utcnow(),
                        run_id=f"engine-batch-{uuid.uuid4()}",
                    )
                    db.add(lead)
                    db.commit()  # Insert one at a time to keep DB state updated
                    db.refresh(lead)
                    new_leads.append(lead)
                    found_count += 1
                    round_added += 1
                    duplicate_streak = 0
                    if found_count >= self.batch_size:
                        break
                if round_added == 0:
                    # No progress this round, rotate queries to avoid stalls
                    self.rotate_search()
        finally:
            asyncio.set_event_loop(None)
            loop.close()
            db.close()
        return new_leads  # Each Lead here includes profile_image_url

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
            # --- CONFIGURATION ---
            # User defined keywords + some defaults
            KEYWORDS = [
                "SaaS Founder", "Marketing Agency Owner", "Ecom Brand Owner", 
                "Web3 Builder", "Solana Developer", "AI Agent Startup",
                "Crypto VC", "DeFi Protocol Founder", "NFT Project Lead"
            ]
            current_kw_index = 0
            consecutive_duplicates = 0
            MAX_DUPLICATES_BEFORE_ROTATE = 15
            TARGET_NEW_LEADS = 50
            
            # Use Universal Search primarily for this targeted "Deep Dive"
            search_collector = UniversalSearchCollector()
            
            # Other collectors ran once at start? Or mixed in? 
            # For now, let's mix standard collectors with the targeted search
            other_collectors = [
                DeFiLlamaCollector(),      
                LaunchpadCollector(),      
                # GithubCollector(), # GitHub is less relevant for "SaaS Founder" keywords
            ]
            
            # STARTUP: Backfill
            await self._backfill_enrichment(db)
            
            self.logger.info(f"🔎 Starting Deep Dive. Target: {TARGET_NEW_LEADS} New Leads.")
            
            # MAIN LOOP
            while self.state["stats"]["new_added"] < TARGET_NEW_LEADS:
                if self.stop_requested: break
                
                self.state["stats"]["loops"] += 1
                loop_idx = self.state["stats"]["loops"]
                
                # ROTATION LOGIC
                current_keyword = KEYWORDS[current_kw_index % len(KEYWORDS)]
                
                # Check Rotate Condition
                if consecutive_duplicates >= MAX_DUPLICATES_BEFORE_ROTATE:
                    current_kw_index += 1
                    consecutive_duplicates = 0 # Reset streak
                    current_keyword = KEYWORDS[current_kw_index % len(KEYWORDS)]
                    self.logger.info(f"🔄 Too many duplicates. Rotating to: {current_keyword}")
                    # Notify UI of Rotation
                    self.update_state(step=f"Rotating to: '{current_keyword}'...")
                    await asyncio.sleep(2) # Visual pause for user to see the status change

                # Update Progress
                pct = min(95, int((self.state["stats"]["new_added"] / TARGET_NEW_LEADS) * 100))
                self.update_state(step=f"Mining: '{current_keyword}' ({self.state['stats']['new_added']}/{TARGET_NEW_LEADS})", progress=pct)
                
                found_any_in_loop = False
                
                # 1. RUN TARGETED SEARCH (High Priority)
                try: 
                    # Construct query with site:twitter.com to ensure profile results
                    query = f"{current_keyword} site:twitter.com"
                    
                    # Pass as override
                    leads = await search_collector.collect(query_override=[query])
                    
                    found_count = len(leads)
                    self.state["stats"]["total_scraped"] += found_count
                    
                    if found_count > 0:
                        found_any_in_loop = True
                        for raw in leads:
                            if self.state["stats"]["new_added"] >= TARGET_NEW_LEADS: break
                            
                            is_new = await self._process_lead(db, raw, run_id)
                            if is_new:
                                consecutive_duplicates = 0 # Reset streak on success
                            else:
                                consecutive_duplicates += 1
                                
                    else:
                        consecutive_duplicates += 5 # Penaltize empty results to rotate faster
                        
                except Exception as e:
                    self.logger.error(f"Search failed: {e}")
                
                 # 2. RUN STANDARD COLLECTORS (Every 3rd loop to keep fresh)
                if loop_idx % 3 == 0: 
                    for c in other_collectors:
                        if self.state["stats"]["new_added"] >= TARGET_NEW_LEADS: break
                        try:
                            self.update_state(step=f"Checking {c.name}...")
                            leads = await c.run(lambda **k: None) # Silent update
                            for raw in leads: 
                                await self._process_lead(db, raw, run_id)
                        except: pass
                
                # Check outcome
                if not found_any_in_loop:
                    self.logger.info("Loop yielded 0 results. Checking limits.")
                    await asyncio.sleep(2)
                
                # Safety Limit
                if self.state["stats"]["loops"] > 200:
                    self.logger.warning("Max loops reached. Stopping.")
                    break
                    
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
                
            # Format Description nicely
            desc_text = raw.extra_data.get("desc")
            if not desc_text:
                # Format extra_data into string
                parts = []
                for k, v in raw.extra_data.items():
                    if k in ['id', 'chainK', 'symbol', 'gecko_id']: continue
                    parts.append(f"{k.capitalize()}: {v}")
                desc_text = ", ".join(parts) if parts else "High-signal project."

            # Create NEW Verified Lead
            lead = Lead(
                project_name=raw.name[:100],
                domain=raw.website,
                normalized_domain=norm_domain,
                twitter_handle=f"@{norm_handle}" if norm_handle else None,
                normalized_handle=norm_handle,
                profile_image_url=raw.profile_image_url
                or (norm_handle and f"https://unavatar.io/twitter/{norm_handle}")
                or f"https://ui-avatars.com/api/?name={urllib.parse.quote(raw.name)}&background=random&color=fff",
                status="New",
                description=str(desc_text)[:500],
                score=raw.extra_data.get('activity_score', 0),
                source_counts=1,
                created_at=datetime.utcnow(),
                run_id=run_id 
            )
            db.add(lead)
            db.commit()
            db.refresh(lead)

            # --- ENRICHMENT TRIGGER ---
            try:
                from enrichment.pipeline import EnrichmentPipeline
                pipeline = EnrichmentPipeline(db)
                await pipeline.process_lead(lead)
                db.commit() # Save Qualification Status
                self.logger.info(f"✨ Ingested & Enriched: {lead.project_name} -> {lead.bucket}")
            except Exception as ev:
                self.logger.error(f"Enrichment Error for {lead.project_name}: {ev}")
            
            self.state["stats"]["new_added"] += 1
            self.state["discovered"] += 1
            return True
            
        except Exception as e:
            db.rollback()
            self.state["stats"]["failed_ingestion"] += 1
            return False

    async def _backfill_enrichment(self, db):
        """Processes any 'New' leads that might have been missed."""
        from enrichment.pipeline import EnrichmentPipeline
        pipeline = EnrichmentPipeline(db)
        
        pending = db.query(Lead).filter(Lead.status == "New").limit(50).all()
        if pending:
             self.logger.info(f"🔄 Backfilling Enrichment for {len(pending)} leads...")
             for p in pending:
                 await pipeline.process_lead(p)
             db.commit()
        try:
            log = RunLog(component=component, level=level, message=message)
            db.add(log)
            db.commit()
        except: pass

engine_instance = StratosphereEngine()
