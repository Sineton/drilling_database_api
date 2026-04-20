"""
Import service - manages import jobs
"""
import uuid
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path
import os


class ImportService:
    """Service for managing import jobs"""
    
    # In-memory job storage (use Redis in production)
    _jobs: Dict[str, Dict[str, Any]] = {}
    
    @classmethod
    def create_job(cls, job_type: str) -> str:
        """Create a new import job"""
        job_id = f"imp_{job_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        
        cls._jobs[job_id] = {
            "job_id": job_id,
            "job_type": job_type,
            "status": "queued",
            "progress": None,
            "result": None,
            "error": None,
            "created_at": datetime.utcnow(),
            "updated_at": None,
            "completed_at": None
        }
        
        return job_id
    
    @classmethod
    def get_job(cls, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job status"""
        return cls._jobs.get(job_id)
    
    @classmethod
    def update_job(
        cls,
        job_id: str,
        status: Optional[str] = None,
        progress: Optional[Dict[str, Any]] = None,
        result: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None
    ):
        """Update job status"""
        if job_id not in cls._jobs:
            return
        
        job = cls._jobs[job_id]
        
        if status:
            job["status"] = status
        if progress:
            job["progress"] = progress
        if result:
            job["result"] = result
        if error:
            job["error"] = error
        
        job["updated_at"] = datetime.utcnow()
        
        if status in ("completed", "failed"):
            job["completed_at"] = datetime.utcnow()
    
    @classmethod
    def list_files_in_folder(
        cls,
        folder_path: str,
        pattern: str = "*.las",
        recursive: bool = True
    ) -> list:
        """List files matching pattern in folder"""
        path = Path(folder_path)
        
        if not path.exists():
            return []
        
        if recursive:
            files = list(path.rglob(pattern))
        else:
            files = list(path.glob(pattern))
        
        return [str(f) for f in files]
    
    @classmethod
    def extract_well_number_from_path(cls, file_path: str) -> Optional[str]:
        """Extract well number from file path (parent folder name)"""
        path = Path(file_path)
        return path.parent.name if path.parent.name else None
