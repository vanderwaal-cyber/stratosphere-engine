import os
from pydantic_settings import BaseSettings
from functools import lru_cache

class Settings(BaseSettings):
    PROJECT_NAME: str = "Stratosphere Lead Engine"
    VERSION: str = "1.0.0"
    
    # Storage
    BASE_DIR: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    # Default to SQLite for local, but prioritize Env Var for prod
    # FORCE FRESH DB: v3.5 to ensure all columns (score, profile_image_url) exist
    DATABASE_URL: str = os.getenv("DATABASE_URL", f"sqlite:///{os.path.join(BASE_DIR, 'stratosphere_v3_5.db')}")
    
    # Collection limits
    MAX_CONCURRENT_REQUESTS: int = 5
    COLLECTOR_TIMEOUT_SECONDS: int = 15
    DAILY_LEAD_TARGET: int = 1000
    
    # Outreach
    COOLDOWN_DAYS: int = 30
    
    # API Keys (Optional with defaults/fallbacks logic in code)
    OPENAI_API_KEY: str = ""
    TELEGRAM_BOT_TOKEN: str = ""
    CMC_API_KEY: str = ""
    
    model_config = {
        "env_file": ".env",
        "extra": "ignore"
    }

@lru_cache()
def get_settings():
    return Settings()
