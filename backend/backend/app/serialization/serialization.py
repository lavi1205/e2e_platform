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

    class Config:
        orm_mode = True
