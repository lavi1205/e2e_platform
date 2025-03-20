from fastapi import FastAPI
from router.routes import router as trip_summary_router
from security.oauth import router as security_router
from router.health import router as health_router
app = FastAPI()

# Include the trip summary router
app.include_router(security_router)
app.include_router(trip_summary_router)
app.include_router(health_router)
