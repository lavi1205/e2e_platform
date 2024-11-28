from fastapi import FastAPI
from backend.backend.app.router import routes as trip_summary_router


app = FastAPI()

# Include the trip summary router
app.include_router(trip_summary_router)
