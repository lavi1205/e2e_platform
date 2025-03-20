from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import select
from datetime import date
from typing import List
import asyncpg

# from database import get_db
# from orm.models import TripSummaryORM
from serialization.serialization import ResponseModel

router = APIRouter()

# PostgreSQL Connection Config
DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/trip_summary"

async def get_db():
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        yield conn
    finally:
        await conn.close()

@router.get("/trips", response_model=ResponseModel, tags=["Trips"])
async def get_trip_summary(
    pickup_date: date = Query(..., description="Date of the trip in YYYY-MM-DD format"),
    db=Depends(get_db)
):
    query = """
        SELECT uuid, pickup_date, total_passenger_count, total_distance, total_fare, avg_trip_distance, avg_fare_amount 
        FROM trip_summary 
        WHERE pickup_date = $1
    """
    result = await db.fetchrow(query, pickup_date)
    
    if result:
        return ResponseModel(status="success", message="Trip summary retrieved successfully", data=dict(result))
    
    raise HTTPException(status_code=404, detail="No data found for this date")
