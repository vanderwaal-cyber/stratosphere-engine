from sqlalchemy import Column, Integer, String, Boolean, DateTime, Float, ForeignKey, Text
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from storage.database import Base

class Lead(Base):
    __tablename__ = "leads"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Dedup Keys (Strict)
    normalized_domain = Column(String, index=True, unique=True, nullable=True) # e.g. "uniswap.org"
    normalized_handle = Column(String, index=True, unique=True, nullable=True) # e.g. "uniswap"
    
    project_name = Column(String, index=True)
    description = Column(Text, nullable=True)
    source_counts = Column(Integer, default=1)
    
    # Contacts (Raw)
    domain = Column(String, nullable=True)
    profile_image_url = Column(String, nullable=True)
    twitter_handle = Column(String, index=True, nullable=True)
    
    # Status
    status = Column(String, default="New", index=True) # New, Enriched, Qualified, Disqualified, Archived
    score = Column(Integer, default=0)
    bucket = Column(String, nullable=True) # READY_TO_DM, NEEDS_ALT_OUTREACH, MANUAL_CHECK
    reject_reason = Column(String, nullable=True)
    
    # Enriched Data
    funding_info = Column(String, nullable=True) # e.g. "$5M Seed"
    telegram_members = Column(Integer, default=0)
    telegram_activity = Column(String, nullable=True) # High, Low, Dead
    
    # Contacts
    email = Column(String, nullable=True)
    discord_url = Column(String, nullable=True)
    telegram_url = Column(String, nullable=True)
    
    # Metadata
    run_id = Column(String, index=True, nullable=True) # Tracks which run generated this lead
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    last_contacted_at = Column(DateTime(timezone=True), nullable=True)

    # Extended Metadata (New)
    telegram_channel = Column(String, index=True, nullable=True) # Normalized identifier
    launch_date = Column(DateTime(timezone=True), nullable=True)
    chains = Column(String, nullable=True) # JSON list or comma-separated
    tags = Column(String, nullable=True) # JSON list or comma-separated
    
    # AI Analysis
    ai_analysis = Column(Text, nullable=True) # JSON or Text blob of analysis
    icebreaker = Column(Text, nullable=True)  # Generated DM opener
    
    # Relations
    sources = relationship("LeadSource", back_populates="lead", cascade="all, delete-orphan")
    logs = relationship("RunLog", back_populates="lead", cascade="all, delete-orphan")

class LeadSource(Base):
    __tablename__ = "lead_sources"
    
    id = Column(Integer, primary_key=True, index=True)
    lead_id = Column(Integer, ForeignKey("leads.id"))
    source_name = Column(String) # coingecko, vc_portfolio, etc.
    source_url = Column(String, nullable=True)
    discovered_at = Column(DateTime(timezone=True), server_default=func.now())
    
    lead = relationship("Lead", back_populates="sources")

class RunLog(Base):
    __tablename__ = "run_logs"
    
    id = Column(Integer, primary_key=True, index=True)
    lead_id = Column(Integer, ForeignKey("leads.id"), nullable=True)
    component = Column(String) # Collector, Enrichment, Scoring
    level = Column(String) # INFO, WARN, ERROR
    message = Column(Text)
    timestamp = Column(DateTime(timezone=True), server_default=func.now())
    
    lead = relationship("Lead", back_populates="logs")
