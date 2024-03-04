from typing import List, Literal

from utils import dto
from utils.log import get_logger
from database.db_layer import DBInterface


db_logger = get_logger("Database")


class DAL:
    def __init__(self, db_type: Literal["postgresql", "mongodb"]) -> None:
        self.db: DBInterface = DBInterface(db_type)


class CarDAL(DAL):
    def process_items(self, items: List[dto.Car]):
        if not items:
            pass

        for item in items:
            if self.db.get_car_by_vin(item.car_vin):
                db_logger.warning(
                    "Item already in database. Vin: %s" % item.car_vin
                )
                continue
            self.db.add_car(item)


class TaskDAL(DAL):
    def reset_tasks_status(self) -> None:
        db_logger.info("Resetting unfinished tasks")
        self.db.reset_tasks_status()

    def create_tasks(self) -> None:
        db_logger.info("Creating new tasks")
        tasks = [
            dto.CreateTask(page_number=i)
            for i in range(1, 11)
        ]
        self.db.bulk_save_tasks(tasks)

    def get_tasks(self, limit: int) -> List[dto.Task]:
        db_logger.info("Getting tasks from DataBase")
        result = []
        for item in self.db.get_idle_tasks(limit):
            task = dto.Task(
                item.id,
                item.page_number,
                item.in_work,
                item.completed
            )
            self.db.update_task(item, in_work=True)
            result.append(task)

        return result


class ResultDAL(DAL):
    def save_results(self, items: List[dto.Result]) -> None:
        if not items:
            return

        db_logger.info("Saving results into DataBase")

        existing_vins = []

        for item in items:
            db_car = self.db.get_car_by_vin(item.car.car_vin)
            if db_car:
                db_logger.warning(
                    "Item already in database. Vin: %s" % item.car.car_vin
                )
            elif item.car.car_vin in existing_vins:
                db_logger.warning(
                    "Item already in database. Vin: %s" % item.car.car_vin
                )
                continue
            else:
                db_car = self.db.add_car(item.car)

            existing_vins.append(item.car.car_vin)

            result = dto.CreateResult(
                task_id=item.task_id,
                car_id=db_car.id
            )
            self.db.add_result(result)
            self.db.update_task(
                self.db.get_task_by_id(item.task_id),
                completed=True
            )
