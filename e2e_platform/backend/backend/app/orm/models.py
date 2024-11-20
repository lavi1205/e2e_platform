from sqlalchemy import Column, Float, String, Date
from sqlalchemy.orm import DeclarativeBase
from datetime import datetime

class Base(DeclarativeBase):
    pass

class User(Base):
    username: str
    password: str


class ActiveSession(Base):
    username: str
    active_token: str
    expiry_time: datetime


class TripSummaryORM(Base):
    __tablename__ = 'trip_summary'

    uuid = Column(String, primary_key=True, unique=True, nullable=False)
    pickup_date = Column(Date, nullable=False)
    total_passenger_count = Column(Float, nullable=False)
    total_distance = Column(Float, nullable=False)
    total_fare = Column(Float, nullable=False)
    avg_trip_distance = Column(Float, nullable=False)
    avg_fare_amount = Column(Float, nullable=False)
