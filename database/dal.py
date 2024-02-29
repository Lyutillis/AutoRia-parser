import os
import sys
from sqlalchemy import and_
from sqlalchemy.orm import Session
from typing import List
from dataclasses import asdict
from datetime import datetime

import envs
from database import models
from utils.dto import Car, Task, Result
from utils.log import get_logger
from database.db_layer import SessionLocal


db_logger = get_logger("Database")


class DAL:
    def __init__(self) -> None:
        self.db: Session = None

    def __enter__(self) -> "DAL":
        self.db = SessionLocal()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.db.close()

    @staticmethod
    def create_database_dump(self) -> None:
        timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        file_path = os.path.join("dumps", f"dump_{timestamp}")
        command = (
            f"pg_dump --no-owner --dbname=postgresql://{envs.POSTGRES_USER}"
            f":{envs.POSTGRES_PASSWORD}@{envs.POSTGRES_HOST}:"
            f"{envs.POSTGRES_PORT}/{envs.POSTGRES_DB} > {file_path}"
        )
        code = os.system(command)
        if code:
            db_logger.error(
                f"Error dumping database: code - {code}, command - {command}"
            )
            sys.exit(10)


class CarDAL(DAL):
    def process_items(self, items: List[Car]):
        if not items:
            pass

        result = self.db.query(models.Car).filter(
            models.Car.car_vin.in_([item.car_vin for item in items])
        ).all()
        result = [car.car_vin for car in result]
        if result:
            for vin in result:
                db_logger.warning("Item already in database. Vin: %s" % vin)
            items = [item for item in items if item.car_vin not in result]

        if items:
            items = [models.Car(**asdict(car)) for car in items]
            self.db.bulk_save_objects(items)
            self.db.commit()


class TaskDAL(DAL):
    def reset_tasks_status(self) -> None:
        db_logger.info("Resetting unfinished tasks")
        self.db.query(models.Task).filter(
            and_(models.Task.in_work == True, models.Task.completed == False)  # noqa
        ).update({"in_work": False})
        self.db.commit()

    def create_tasks(self) -> None:
        db_logger.info("Creating new tasks")
        tasks = [
            models.Task(page_number=i)
            for i in range(1, 11)
        ]
        self.db.bulk_save_objects(tasks)
        self.db.commit()

    def get_tasks(self, limit: int) -> List[Task]:
        db_logger.info("Getting tasks from DataBase")
        result = [
            Task(
                item.id,
                item.page_number,
                item.in_work,
                item.completed
            )
            for item in
            self.db.query(models.Task).filter(models.Task.in_work == False).limit(limit).all()  # noqa
        ]
        self.db.query(models.Task).where(
            models.Task.id.in_(
                [item.id for item in result]
            )
        ).update({"in_work": True})
        self.db.commit()
        return result


class ResultDAL(DAL):
    def save_results(self, items: List[Result]) -> None:
        if not items:
            return

        db_logger.info("Saving results into DataBase")

        existing_vins = []

        item_vins = [item.car.car_vin for item in items]

        db_vins = self.db.query(models.Car).filter(
            models.Car.car_vin.in_(item_vins)
        ).all()

        db_vins_dict = {car.car_vin: car.id for car in db_vins}

        for item in items:
            if item.car.car_vin in db_vins_dict.keys():
                db_logger.warning(
                    "Item already in database. Vin: %s" % item.car.car_vin
                )
                car_id = db_vins_dict[item.car.car_vin]
            elif item.car.car_vin in existing_vins:
                db_logger.warning(
                    "Item already in database. Vin: %s" % item.car.car_vin
                )
                continue
            else:
                car = models.Car(
                    **asdict(item.car)
                )
                self.db.add(car)
                self.db.flush()
                self.db.refresh(car)
                car_id = car.id

            existing_vins.append(item.car.car_vin)

            result = models.Result(
                task_id=item.task_id,
                car_id=car_id
            )
            self.db.add(result)
            self.db.query(models.Task).filter(
                models.Task.id == item.task_id
            ).update({"completed": True})

        self.db.commit()
