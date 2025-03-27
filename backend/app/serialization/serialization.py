from pydantic import BaseModel
from datetime import date
from typing import Optional

class TripSummary(BaseModel):
    uuid: str
    pickup_date: date
    total_passenger_count: float
    total_distance: float
    total_fare: float
    avg_trip_distance: float
    avg_fare_amount: float

class ResponseModel(BaseModel):
    status: str
    message: str
    data: TripSummary | None = None

class HealthCheck(BaseModel):
    """Response model to validate and return when performing a health check."""

    status: str = "OK"
