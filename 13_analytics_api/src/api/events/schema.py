from pydantic import BaseModel, Field
from typing import List, Optional

class EventSchema(BaseModel):
    id: int
    page: Optional[str] = "" 
    description: Optional[str] = ""

class EventListSchema(BaseModel):
    results: List[EventSchema]
    count: int

class EventCreateSchema(BaseModel):
    page: str
    description: Optional[str] = Field(default="")

class EventUpdateSchema(BaseModel):
    description: str

