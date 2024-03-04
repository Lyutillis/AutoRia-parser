from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    ForeignKey,
    DateTime,
    Boolean
)

from database.config import Base


class Car(Base):
    __tablename__ = "car"

    id = Column(Integer, primary_key=True, index=True)
    url = Column(String(255), nullable=False)
    title = Column(String(255), nullable=False)
    price_usd = Column(Float, nullable=False)
    odometer = Column(Float, nullable=False)
    username = Column(String(255), nullable=False)
    phone_number = Column(String(255), nullable=False)
    image_url = Column(String(255), nullable=True)
    images_count = Column(Integer, nullable=False)
    car_number = Column(String(255), nullable=True)
    car_vin = Column(String(255), nullable=False, unique=True)
    datetime_found = Column(DateTime(), nullable=False)


class Task(Base):
    __tablename__ = "task"

    id = Column(Integer, primary_key=True, index=True)
    page_number = Column(Integer, nullable=False)
    in_work = Column(Boolean, default=False)
    completed = Column(Boolean, default=False)


class Result(Base):
    __tablename__ = "result"

    id = Column(Integer, primary_key=True, index=True)
    task_id = Column(Integer, ForeignKey("task.id"))
    car_id = Column(Integer, ForeignKey("car.id"))
