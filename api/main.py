from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session
from sqlalchemy import desc, text
from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel
from storage.database import get_db, Base, engine
from storage.models import Lead as LeadModel, RunLog
from core.engine import engine_instance
import os
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

# Rate Limiter
limiter = Limiter(key_func=get_remote_address)
app = FastAPI(title="Stratosphere API", version="2.0.0")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

@app.on_event("startup")
async def startup_db():
    print("Running DB Startup...")
    Base.metadata.create_all(bind=engine)
    
    # Robust Migration (Run ID)
    with engine.connect() as conn:
        try:
            # 1. Try Postgres (Ideal for Prod)
            try:
                conn.execute(text("ALTER TABLE leads ADD COLUMN IF NOT EXISTS run_id VARCHAR"))
                conn.commit()
            except: pass
            
            try:
                conn.execute(text("ALTER TABLE leads ADD COLUMN IF NOT EXISTS profile_image_url VARCHAR"))
                conn.commit()
            except: pass

            try:
                conn.execute(text("ALTER TABLE leads ADD COLUMN IF NOT EXISTS score INTEGER DEFAULT 0"))
                conn.commit()
            except: pass

            try:
                conn.execute(text("ALTER TABLE leads ADD COLUMN IF NOT EXISTS ai_analysis TEXT"))
                conn.commit()
            except: pass

            try:
                conn.execute(text("ALTER TABLE leads ADD COLUMN IF NOT EXISTS icebreaker TEXT"))
                conn.commit()
            except: pass

            print("Migration (Postgres) attempts complete.")
        except Exception:
            # 2. Try SQLite (Local) - No IF NOT EXISTS
            try:
                conn.execute(text("ALTER TABLE leads ADD COLUMN run_id VARCHAR"))
                conn.commit()
            except: pass
            
            try:
                conn.execute(text("ALTER TABLE leads ADD COLUMN score INTEGER DEFAULT 0"))
                conn.commit()
            except: pass
            
            try:
                conn.execute(text("ALTER TABLE leads ADD COLUMN profile_image_url VARCHAR"))
                conn.commit()
            except: pass
            
            try:
                conn.execute(text("ALTER TABLE leads ADD COLUMN ai_analysis TEXT"))
                conn.commit()
            except: pass

            try:
                conn.execute(text("ALTER TABLE leads ADD COLUMN icebreaker TEXT"))
                conn.commit()
            except: pass
            
            print("Migration (SQLite) attempts complete.")

# Schemas
class LeadBase(BaseModel):
    id: int
    project_name: str
    twitter_handle: Optional[str] = None
    domain: Optional[str] = None
    funding_info: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = "New"  # Loosened
    score: Optional[int] = 0       # Loosened
    bucket: Optional[str] = None
    source_counts: int = 1
    profile_image_url: Optional[str] = None
    telegram_url: Optional[str] = None
    discord_url: Optional[str] = None
    email: Optional[str] = None
    reject_reason: Optional[str] = None
    created_at: Optional[datetime] = None
    run_id: Optional[str] = None
    ai_analysis: Optional[str] = None
    icebreaker: Optional[str] = None

    class Config:
        from_attributes = True

@app.get("/leads", response_model=List[LeadBase])
@limiter.limit("60/minute")
async def read_leads(request: Request, skip: int = 0, limit: int = 100, bucket: Optional[str] = None, run_id: Optional[str] = None, created_after: Optional[datetime] = None, db: Session = Depends(get_db)):
    try:
        query = db.query(LeadModel)
        if bucket:
            query = query.filter(LeadModel.bucket == bucket)
        
        # Priority Filter: Run ID (Strict)
        if run_id:
            query = query.filter(LeadModel.run_id == run_id)
        elif created_after:
            query = query.filter(LeadModel.created_at >= created_after)
            
        # Default Sort: Activity Score DESC -> Recency DESC
        leads = query.order_by(LeadModel.score.desc(), LeadModel.created_at.desc()).offset(skip).limit(limit).all()
        return leads
    except Exception as e:
        import traceback
        traceback.print_exc()
        # Return detailed error so dashboard can show it
        raise HTTPException(status_code=500, detail=f"DB Error: {str(e)}")

# Serve Assets
try:
    if os.path.exists("stratosphere/frontend/assets"):
        app.mount("/assets", StaticFiles(directory="stratosphere/frontend/assets"), name="assets")
    elif os.path.exists("frontend/assets"):
        app.mount("/assets", StaticFiles(directory="frontend/assets"), name="assets")
except Exception as e:
    print(f"Warning: Could not mount assets: {e}")

# CORS - Allow Vercel Frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Production: Restrict to your Vercel domain later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/dashboard")

@app.get("/health")
def health_check():
    return {"status": "ok", "port_check": "accessible"}

class RunRequest(BaseModel):
    mode: str = "fresh"
    run_id: Optional[str] = None
    auto_pilot: bool = False

@app.post("/pipeline/run")
@limiter.limit("5/minute")
async def trigger_pipeline(request: Request, req: RunRequest = RunRequest(), background_tasks: BackgroundTasks = BackgroundTasks()):
    try:
        if engine_instance.state["state"] == "running":
            return {"status": "busy", "message": "Pipeline already running"}
            
        # Update engine state with auto_pilot pref
        engine_instance.state["auto_pilot"] = req.auto_pilot
        
        background_tasks.add_task(engine_instance.run, mode=req.mode, run_id=req.run_id)
        return {"status": "started", "message": f"Pipeline running in background ({req.mode})", "run_id": req.run_id}
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/pipeline/stop")
async def stop_pipeline():
    engine_instance.stop()
    return {"status": "stopping", "message": "Stop signal sent"}

@app.get("/pipeline/status")
def get_pipeline_status():
    return engine_instance.state

from core.ai_drafting import DMDrafter
from core.enrichment import EnrichmentEngine

@app.post("/api/leads/{lead_id}/analyze")
async def analyze_lead(lead_id: int, db: Session = Depends(get_db)):
    lead = db.query(LeadModel).filter(LeadModel.id == lead_id).first()
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
        
    try:
        # 1. Deep Enrichment (Web Scraping)
        if lead.domain and "http" in lead.domain:
            enricher = EnrichmentEngine()
            enriched_data = await enricher.enrich_url(lead.domain)
            
            # Update Lead with found contacts
            if enriched_data.get("email"): lead.email = enriched_data["email"]
            if enriched_data.get("telegram_url"): lead.telegram_url = enriched_data["telegram_url"]
            if enriched_data.get("discord_url"): lead.discord_url = enriched_data["discord_url"]
            # If we found a handle and didn't have one, save it
            if enriched_data.get("twitter_handle") and not lead.twitter_handle:
                 lead.twitter_handle = enriched_data["twitter_handle"]

        # 2. Real AI Analysis via NeuroLink (GPT-4)
        drafter = DMDrafter()
        
        # Prepare Context
        project_context = {
            "project_name": lead.project_name,
            "description": lead.description or "",
            "telegram_found": bool(lead.telegram_url), # Inform AI
            "email_found": bool(lead.email)
        }
        
        # Generate
        result = drafter.generate_analysis(project_context)
        
        # Save
        lead.ai_analysis = result.get("ai_analysis", "")
        lead.icebreaker = result.get("icebreaker", "")
            
        db.commit()
        return {
            "id": lead.id, 
            "ai_analysis": lead.ai_analysis, 
            "icebreaker": lead.icebreaker,
            "email": lead.email,
            "telegram": lead.telegram_url,
            "discord": lead.discord_url
        }
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

class StatusUpdate(BaseModel):
    status: Optional[str] = None
    bucket: Optional[str] = None

@app.post("/api/leads/{lead_id}/status")
async def update_lead_status(lead_id: int, update: StatusUpdate, db: Session = Depends(get_db)):
    lead = db.query(LeadModel).filter(LeadModel.id == lead_id).first()
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    
    if update.status:
        lead.status = update.status
    if update.bucket:
        lead.bucket = update.bucket
        
    db.commit()
    return {"id": lead.id, "status": lead.status, "bucket": lead.bucket}

@app.get("/dashboard", response_class=HTMLResponse)
def read_dashboard():
    # Defensive path check
    path = "stratosphere/frontend/index.html"
    if not os.path.exists(path):
        if os.path.exists("frontend/index.html"):
            path = "frontend/index.html"
    
    if os.path.exists(path):
        with open(path, "r") as f:
            return f.read()
    return "Dashboard file not found."



@app.get("/leads/stats")
async def read_stats(db: Session = Depends(get_db)):
    total = db.query(LeadModel).count()
    ready = db.query(LeadModel).filter(LeadModel.bucket == "READY_TO_DM").count()
    alt = db.query(LeadModel).filter(LeadModel.bucket == "NEEDS_ALT_OUTREACH").count()
    
    # New Metrics
    telegram_count = db.query(LeadModel).filter((LeadModel.telegram_channel != None) | (LeadModel.telegram_url != None)).count()
    
    # Source Breakdown
    from sqlalchemy import func
    sources_query = db.query(LeadModel.source, func.count(LeadModel.source)).group_by(LeadModel.source).all()
    sources = {s: c for s, c in sources_query}
    
    return {
        "total_leads": total,
        "ready_to_dm": ready,
        "needs_alt_outreach": alt,
        "telegram_leads": telegram_count,
        "sources": sources
    }

from fastapi.responses import StreamingResponse
import io
import csv

@app.get("/leads/export")
async def export_leads(run_id: Optional[str] = None, db: Session = Depends(get_db)):
    query = db.query(LeadModel)
    if run_id:
        query = query.filter(LeadModel.run_id == run_id)
    
    leads = query.order_by(desc(LeadModel.created_at)).limit(2000).all()
    
    stream = io.StringIO()
    writer = csv.writer(stream)
    
    # Headers
    writer.writerow(["ID", "Project", "Website", "Twitter", "Status", "Bucket", "Email", "Run ID", "Found At", "AI Analysis"])
    
    for l in leads:
        writer.writerow([
            l.id, l.project_name, l.domain, l.twitter_handle, 
            l.status, l.bucket, l.email, l.run_id, l.created_at, l.ai_analysis
        ])
        
    stream.seek(0)
    response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
    response.headers["Content-Disposition"] = f"attachment; filename=leads_export_{run_id or 'all'}.csv"
    return response

# Aliases for User Requirements
app.add_api_route("/api/generate", trigger_pipeline, methods=["POST"])
app.add_api_route("/api/status", get_pipeline_status, methods=["GET"])
app.add_api_route("/api/leads", read_leads, methods=["GET"])
app.add_api_route("/api/leads/stats", read_stats, methods=["GET"])
