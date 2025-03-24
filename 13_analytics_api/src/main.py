from fastapi import FastAPI
from api.events import router as event_router

app = FastAPI()
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