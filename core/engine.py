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

    def _load_rotation_state(self):
        import json
        import os
        STATE_FILE = "core/niche_state.json"
        try:
            if os.path.exists(STATE_FILE):
                with open(STATE_FILE, 'r') as f:
                    return json.load(f)
        except Exception:
            pass
        return {"index": 0}

    def _save_rotation_state(self, index):
        import json
        STATE_FILE = "core/niche_state.json"
        try:
            with open(STATE_FILE, 'w') as f:
                json.dump({"index": index}, f)
        except Exception:
            pass

    async def _run_collection_phase(self, mode, run_id):
        db = SessionLocal()
        try:
            # --- CONFIGURATION ---
            # Expanded Professional User-Agents (Mac, Windows, iOS, Android)
            USER_AGENTS = [
                # Desktop
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/121.0",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
                # Mobile / Tablet
                "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
                "Mozilla/5.0 (iPad; CPU OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
                "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
                "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ]

            # --- NICHE SCHEDULE & VARIANTS ---
            NICHE_SCHEDULE = [
                # Week 1: Crypto & Web3
                "Solana Validators", "DeFi Protocol Leads", "NFT Market Makers", "DAO Treasurers", "Crypto VC Partners", "GameFi Developers", "Launchpad Founders",
                # Week 2: B2B Services
                "M&A Advisors", "Fractional CFOs", "Executive Coaches", "Logistics Directors", "HR Tech Founders", "IP Attorneys", "Supply Chain VPs",
                # Week 3: High-Ticket Tech
                "AI Automation Agencies", "Cybersecurity Firms", "EdTech Founders", "Fintech Startup Leads", "BioTech Executives", "CleanTech Directors", "AdTech Strategists",
                # Week 4: Niche Agencies
                "Solar Energy CEOs", "Medical Spa Owners", "Luxury Real Estate", "Yacht Brokerages", "Private Jet Charters", "High-End Interior Design", "Ecom Aggregators"
            ]
            
            # Map strict keywords to broader/alternative search terms if blocked or dry
            NICHE_VARIANTS = {
                "Solana Validators": ["Solana Node Operators", "SOL Validators", "Solana Staking Providers", "Solana RPC Nodes"],
                "DeFi Protocol Leads": ["DeFi Founders", "DEX Developers", "Liquidity Protocol Owners", "Yield Farming Devs"],
                "NFT Market Makers": ["NFT Liquidity Providers", "NFT Floor Sweepers", "Digital Asset Traders"],
                "DAO Treasurers": ["DAO Signers", "Governance Delegates", "Web3 Treasury Managers"],
                "Crypto VC Partners": ["Web3 Investors", "Blockchain Venture Capital", "Crypto Angels"],
                # Fallback generator for others
            }
            
            # Load Persistent State
            state_data = self._load_rotation_state()
            current_index = state_data.get("index", 0) % len(NICHE_SCHEDULE)
            
            # Select Keyword for THIS Run
            current_keyword = NICHE_SCHEDULE[current_index]
            TARGET_NEW_LEADS = 50
            
            search_collector = UniversalSearchCollector()
            
            self.logger.info(f"🔎 Manual Trigger. Selected Niche: '{current_keyword}' (Index {current_index}). Target: {TARGET_NEW_LEADS}")
            
            # Update UI immediately
            self.update_state(step=f"Scanning Niche: '{current_keyword}'", progress=0)
            
            # MAIN BATCH LOOP
            while self.state["stats"]["new_added"] < TARGET_NEW_LEADS:
                if self.stop_requested: break
                
                self.state["stats"]["loops"] += 1
                
                # Update Progress
                pct = int((self.state["stats"]["new_added"] / TARGET_NEW_LEADS) * 100)
                self.update_state(step=f"Scanning Niche: '{current_keyword}' ({self.state['stats']['new_added']}/{TARGET_NEW_LEADS})", progress=pct)
                
                # Human Jitter: Slower 4-9s sleep for maximum stealth
                sleep_time = random.uniform(4, 9)
                self.logger.info(f"😴 Human Jitter: Sleeping for {sleep_time:.1f}s...")
                await asyncio.sleep(sleep_time)
                
                # Header Rotation (Pick one for this request)
                current_ua = random.choice(USER_AGENTS)
                
                # Construct Query List (Primary + Variants if needed)
                queries_to_try = [f'"{current_keyword}" site:twitter.com']
                
                # Add intelligent variants if defined
                if current_keyword in NICHE_VARIANTS:
                    for v in NICHE_VARIANTS[current_keyword]:
                        queries_to_try.append(f'"{v}" site:twitter.com')
                else:
                    # Generic Fallbacks
                    queries_to_try.append(f'"{current_keyword}" "Founder" site:twitter.com')
                    queries_to_try.append(f'"{current_keyword}" "Owner" site:twitter.com')

                # Randomize slightly to avoid pattern detection
                query = random.choice(queries_to_try)

                try:
                    leads = await search_collector.collect(query_override=[query])
                    found_count = len(leads)
                    
                    found_new_in_batch = False
                    
                    if found_count > 0:
                        for raw in leads:
                            if self.state["stats"]["new_added"] >= TARGET_NEW_LEADS: break
                            is_new = await self._process_lead(db, raw, run_id)
                            if is_new:
                                found_new_in_batch = True
                                pct = int((self.state["stats"]["new_added"] / TARGET_NEW_LEADS) * 100)
                                self.update_state(step=f"Scanning Niche: '{current_keyword}' ({self.state['stats']['new_added']}/{TARGET_NEW_LEADS})", progress=pct)

                    if not found_new_in_batch:
                        # If Primary failed, try a specific Deep Variant immediately if we haven't just tried it
                        if found_count == 0:
                            self.logger.warning(f"⚠️ Zero results for '{query}'. Attempting Deep Variant...")
                            self.update_state(step=f"Deep Scan: '{current_keyword}' (Variant Retry...)", progress=pct)
                            
                            # Pick a different variant from the list
                            retry_query = random.choice(queries_to_try) 
                            if retry_query == query and len(queries_to_try) > 1:
                                # Try to pick a different one
                                for q in queries_to_try:
                                    if q != query: 
                                        retry_query = q
                                        break
                            
                            await asyncio.sleep(3) # Pause before retry
                            leads = await search_collector.collect(query_override=[retry_query])
                            found_count = len(leads)
                            
                            if found_count > 0:
                                for raw in leads:
                                    if self.state["stats"]["new_added"] >= TARGET_NEW_LEADS: break
                                    if await self._process_lead(db, raw, run_id): found_new_in_batch = True

                        if not found_new_in_batch:
                            self.logger.info("Batch yielded no new leads. Sleeping constraints.")
                            await asyncio.sleep(2)
                        
                except Exception as e:
                    self.logger.error(f"Search Loop Error: {e}")
                    await asyncio.sleep(1)

                # Safety Limit
                if self.state["stats"]["loops"] > 40: # Increased limit for variants
                    self.logger.warning(f"Batch limit reached for {current_keyword}.")
                    break
            
            # End of Run: Advance Cursor
            next_index = (current_index + 1) % len(NICHE_SCHEDULE)
            self._save_rotation_state(next_index)
            self.logger.info(f"✅ Batch Complete. Rotated Cursor to Index {next_index}.")
            
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
