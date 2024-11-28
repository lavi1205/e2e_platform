from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select
from datetime import date
from typing import List

from database import get_db
from orm.models import TripSummaryORM
from serilazition.serialization import TripSummary

router = APIRouter()

# Dependency to get DB session
def get_db_session():
    db = get_db()
    try:
        yield db
    finally:
        db.close()

# Define the router for retrieving summaries based on a specific pickup date
@router.get("/trip_summary/", response_model=List[TripSummary])
def get_trip_summary(pickup_date: date, db: Session = Depends(get_db_session)):
    # Query the database for summaries matching the pickup date
    results = db.execute(select(TripSummaryORM).where(TripSummaryORM.pickup_date == pickup_date)).scalars().all()

    if not results:
        raise HTTPException(status_code=404, detail="No trip summaries found for the given pickup date")

    # Return the results as a list of serialized TripSummary objects
    return results
