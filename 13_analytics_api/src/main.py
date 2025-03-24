from contextlib import asynccontextmanager
from api.db.session import init_db
from fastapi import FastAPI
from api.events import router as event_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    # before app startup
    init_db()
    yield
    # after app startup


app = FastAPI(lifespan=lifespan)
app.include_router(event_router, prefix="/api/events")

@app.get("/")
def index():
    return {
        "message": "Hello Docker Hello" 
    }

@app.get("/healthz")
def read_api_health():
    return {
        "status": "OK" 
    }