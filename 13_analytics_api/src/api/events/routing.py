from fastapi import APIRouter, Depends
from api.db.session import get_session
from sqlmodel import Session

from .models import (
    EventModel, 
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
def get_event(event_id: int) -> EventModel:
    # a single rows
    return {
        "id": event_id
    }

# POST METHOD
@router.post("/", response_model=EventModel)
def create_event(
    payload: EventCreateSchema,
    session: Session = Depends(get_session)
):
    data = payload.model_dump() # payload -> dict -> pydantic
    obj = EventModel.model_validate(data)
    session.add(obj)
    session.commit()
    session.refresh(obj)

    return obj

# PUT METHOD
@router.put("/{event_id}")
def update_event(event_id: int, payload: EventUpdateSchema) -> EventModel:
    data = payload.model_dump() # payload -> dict -> pydantic
    return {'id':event_id,**data}

# DELETE METHOD
@router.delete("/{event_id}")
def update_event(event_id: int) -> EventModel:
    return {"id": event_id}