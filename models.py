from typing import Dict, List, Optional
from pydantic import BaseModel

class Carrier(BaseModel):
    iata: str
    name: str


class Route(BaseModel):
    carriers: List[Carrier]
    iata: str
    km: int
    min: int


class Airport(BaseModel):
    city_name: Optional[str] = None
    continent: Optional[str] = None
    country: Optional[str] = None
    country_code: Optional[str] = None
    display_name: Optional[str] = None
    elevation: Optional[int] = None
    iata: str
    icao: Optional[str] = None
    latitude: Optional[str] = None
    longitude: Optional[str] = None
    name: Optional[str] = None
    routes: List[Route] = []
    timezone: Optional[str] = None
