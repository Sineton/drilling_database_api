"""
Datasets endpoints (draft): stuck pipe training dataset builder.
"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from ..database import get_db
from ..schemas.analytics import DatasetBuildRequest, DatasetBuildResponse
from ..services.dataset_service import DatasetService

router = APIRouter(prefix="/datasets", tags=["Datasets"])


@router.post("/stuck-pipe-training", response_model=DatasetBuildResponse)
def build_stuck_pipe_training_dataset(
    payload: DatasetBuildRequest,
    db: Session = Depends(get_db),
    preview: bool = Query(True, description="Черновой режим: вернуть выборку прямо в ответе"),
):
    service = DatasetService(db)
    result = service.build_stuck_pipe_dataset(
        field=payload.field,
        well_numbers=payload.well_numbers,
        before_minutes=payload.before_minutes,
        after_minutes=payload.after_minutes,
        include_negative=payload.include_negative,
        negatives_per_positive=payload.negatives_per_positive,
        max_samples=payload.max_samples,
    )

    if preview:
        return DatasetBuildResponse(**result)

    return DatasetBuildResponse(**result)
