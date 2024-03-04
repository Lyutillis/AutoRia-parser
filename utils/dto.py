from dataclasses import dataclass
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


@dataclass
class Task:
    id: int
    page_number: int
    in_work: bool
    completed: bool


@dataclass
class CreateTask:
    page_number: int


@dataclass
class Result:
    task_id: int
    car: Car


@dataclass
class CreateResult:
    task_id: int
    car_id: int
