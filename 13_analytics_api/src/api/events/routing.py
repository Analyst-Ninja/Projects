from fastapi import APIRouter
from .schema import (
    EventSchema, 
    EventListSchema,
    EventCreateSchema,
    EventUpdateSchema
)

router = APIRouter()

# GET METHODS

@router.get("/")
def read_events() -> EventListSchema:
    # a bunch of rows
    return {
        "results":[
            {"id":1},
            {"id":2},
            {"id":3},
            {"id":4}
        ],
        "count" : 4
    }

@router.get("/{event_id}")
def get_event(event_id: int) -> EventSchema:
    # a single rows
    return {
        "id": event_id
    }

# POST METHOD
@router.post("/")
def create_event(payload: EventCreateSchema) -> EventSchema:
    data = payload.model_dump() # payload -> dict -> pydantic
    return {'id':123,**data}

# PUT METHOD
@router.put("/{event_id}")
def update_event(event_id: int, payload: EventUpdateSchema) -> EventSchema:
    data = payload.model_dump() # payload -> dict -> pydantic
    return {'id':event_id,**data}

# DELETE METHOD
@router.delete("/{event_id}")
def update_event(event_id: int) -> EventSchema:
    return {"id": event_id}