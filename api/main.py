from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session
from sqlalchemy import desc
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

# Ensure tables exist (Zero-setup)
Base.metadata.create_all(bind=engine)

# Rate Limiter
limiter = Limiter(key_func=get_remote_address)
app = FastAPI(title="Stratosphere API", version="2.0.0")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

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

@app.post("/pipeline/run")
@limiter.limit("5/minute")
async def trigger_pipeline(request: Request, req: RunRequest = RunRequest(), background_tasks: BackgroundTasks = BackgroundTasks()):
    if engine_instance.state["state"] == "running":
        return {"status": "busy", "message": "Pipeline already running"}
        
    background_tasks.add_task(engine_instance.run, mode=req.mode)
    return {"status": "started", "message": f"Pipeline running in background ({req.mode})"}

@app.post("/pipeline/stop")
async def stop_pipeline():
    engine_instance.stop()
    return {"status": "stopping", "message": "Stop signal sent"}

@app.get("/pipeline/status")
def get_pipeline_status():
    return engine_instance.state

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

# Schemas
class LeadBase(BaseModel):
    project_name: str
    twitter_handle: Optional[str] = None
    domain: Optional[str] = None
    funding_info: Optional[str] = None
    description: Optional[str] = None
    status: str
    score: int
    bucket: Optional[str] = None
    source_counts: int = 1
    telegram_url: Optional[str] = None
    discord_url: Optional[str] = None
    email: Optional[str] = None
    reject_reason: Optional[str] = None
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True

@app.get("/leads", response_model=List[LeadBase])
@limiter.limit("60/minute")
async def read_leads(request: Request, skip: int = 0, limit: int = 100, bucket: Optional[str] = None, db: Session = Depends(get_db)):
    query = db.query(LeadModel)
    if bucket:
        query = query.filter(LeadModel.bucket == bucket)
    leads = query.order_by(LeadModel.created_at.desc()).offset(skip).limit(limit).all()
    return leads

@app.get("/leads/stats")
async def read_stats(db: Session = Depends(get_db)):
    total = db.query(LeadModel).count()
    ready = db.query(LeadModel).filter(LeadModel.bucket == "READY_TO_DM").count()
    alt = db.query(LeadModel).filter(LeadModel.bucket == "NEEDS_ALT_OUTREACH").count()
    return {
        "total_leads": total,
        "ready_to_dm": ready,
        "needs_alt_outreach": alt
    }

# Aliases for User Requirements
app.add_api_route("/api/generate", trigger_pipeline, methods=["POST"])
app.add_api_route("/api/status", get_pipeline_status, methods=["GET"])
app.add_api_route("/api/leads", read_leads, methods=["GET"])
app.add_api_route("/api/leads/stats", read_stats, methods=["GET"])
