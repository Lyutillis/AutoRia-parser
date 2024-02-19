from dataclasses import dataclass, astuple, fields
from datetime import datetime


@dataclass
class Car:
    url: str
    title: str
    price_usd: float
    odometer: float
    username: str
    phone_number: str
    image_url: str
    images_count: int
    car_number: str
    car_vin: str
    datetime_found: datetime
