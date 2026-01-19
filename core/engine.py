  from datetime import datetime, timedelta

from storage.database import SessionLocal
from storage.models import Lead
from core.logger import app_logger


class Engine:
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
            "enriched": 0,
            "ready_to_dm": 0,
            "needs_ai": 0,
            "current_step": "Ready",
            "current_target": "",
            "progress": 0,
            "stats": {
                "new_added": 0,
                "duplicates_skipped": 0,
            },
        }

    def update_state(self, step=None, progress=None, target=None):
        if step is not None:
            self.state["current_step"] = step
        if progress is not None:
            self.state["progress"] = progress
        if target is not None:
            self.state["current_target"] = target

        self.state["updated_at"] = datetime.utcnow().isoformat()

    async def _run_logic(self, mode="refresh"):
        db = SessionLocal()
        try:
            # 0. Auto-Cleanup (7 Days)
            cutoff = datetime.utcnow() - timedelta(days=7)
            deleted = (
                db.query(Lead)
                .filter(Lead.created_at < cutoff)
                .delete(synchronize_session=False)
            )

            if deleted and deleted > 0:
                self.logger.info(f"Cleaned up {deleted} old leads.")
                db.commit()

            # TODO: your collection + enrichment logic continues here
            self.update_state(step="Collecting", progress=10)

        except Exception as e:
            self.logger.exception(f"Engine run failed: {e}")
            raise
        finally:
            db.close()

    def stop(self):
        self.stop_requested = True
        self.update_state(step="Stopping", progress=self.state.get("progress", 0))


engine_instance = Engine()
