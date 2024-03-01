import abc
from dataclasses import asdict
from datetime import datetime
import os
import sys
from typing import Literal, Optional, Union, List
from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from mongoengine import connect

from database import models, mongo_models
from utils.dto import Car, Task, CreateTask, Result
import envs


def get_db_class(
    db_type: Literal["postgresql", "mongodb"]
) -> Optional[Union["PostgreSQL", "MongoDB"]]:
    if db_type == "postgresql":
        return PostgreSQL

    if db_type == "mongodb":
        return MongoDB

    return None


class DatabaseABC(abc.ABC):

    @abc.abstractmethod
    def get_car_by_vin(self, vin: str) -> Optional[models.Car]:
        pass

    @abc.abstractmethod
    def bulk_save_cars(
        self,
        objects: List[Car]
    ) -> None:
        pass

    @abc.abstractmethod
    def bulk_save_tasks(
        self,
        objects: List[CreateTask]
    ) -> None:
        pass

    @abc.abstractmethod
    def bulk_save_results(
        self,
        objects: List[Result]
    ) -> None:
        pass

    @abc.abstractmethod
    def reset_tasks_status(self) -> None:
        pass

    @abc.abstractmethod
    def get_idle_tasks(self, limit: int) -> List[models.Task]:
        pass

    @abc.abstractmethod
    def update_task(self, task: Task, **kwargs) -> models.Task:
        pass

    @abc.abstractmethod
    def add_car(
        self,
        object: Car
    ) -> models.Car:
        pass

    @abc.abstractmethod
    def add_task(
        self,
        object: CreateTask
    ) -> models.Task:
        pass

    @abc.abstractmethod
    def add_result(
        self,
        object: Result
    ) -> models.Result:
        pass

    @abc.abstractmethod
    def get_task_by_id(self, id: int) -> models.Task:
        pass


class PostgreSQL(DatabaseABC):

    def __init__(self) -> None:
        self.engine = create_engine(
            (
                f"postgresql://{envs.POSTGRES_USER}:{envs.POSTGRES_PASSWORD}"
                f"@{envs.POSTGRES_HOST}/{envs.POSTGRES_DB}"
            )
        )
        self.SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine
        )
        models.Base.metadata.create_all(self.engine)

    def get_car_by_vin(self, vin: str) -> Optional[models.Car]:
        with self.SessionLocal() as db:
            return db.query(models.Car).filter(
                models.Car.car_vin == vin
            ).first()

    def bulk_save_cars(
        self,
        objects: List[Car]
    ) -> None:
        with self.SessionLocal() as db:
            db.bulk_save_objects(
                [
                    models.Car(**asdict(car))
                    for car in objects
                ]
            )
            db.commit()

    def bulk_save_tasks(
        self,
        objects: List[CreateTask]
    ) -> None:
        with self.SessionLocal() as db:
            db.bulk_save_objects(
                [
                    models.Task(**asdict(task))
                    for task in objects
                ]
            )
            db.commit()

    def bulk_save_results(
        self,
        objects: List[Result]
    ) -> None:
        with self.SessionLocal() as db:
            db.bulk_save_objects(
                [
                    models.Result(**asdict(result))
                    for result in objects
                ]
            )
            db.commit()

    def reset_tasks_status(self) -> None:
        with self.SessionLocal() as db:
            db.query(models.Task).filter(
                and_(models.Task.in_work == True, models.Task.completed == False)  # noqa
            ).update({"in_work": False})
            db.commit()

    def get_idle_tasks(self, limit: int) -> List[models.Task]:
        with self.SessionLocal() as db:
            return db.query(models.Task).filter(
                models.Task.in_work == False  # noqa
            ).limit(limit).all()

    def update_task(self, task: Task, **kwargs) -> models.Task:
        with self.SessionLocal() as db:
            db.query(models.Task).filter(
                models.Task.id == task.id
            ).update(kwargs)
            db.commit()
        return task

    def add_car(
        self,
        object: Car
    ) -> models.Car:
        with self.SessionLocal() as db:
            car = models.Car(**asdict(object))
            db.add(car)
            db.commit()
            return db.query(models.Car).fitler(models.Car.id == car.id).first()

    def add_task(
        self,
        object: CreateTask
    ) -> models.Task:
        with self.SessionLocal() as db:
            task = models.Task(**asdict(object))
            db.add(task)
            db.commit()
            return db.query(models.Task).fitler(
                models.Task.id == task.id
            ).first()

    def add_result(
        self,
        object: Result
    ) -> models.Result:
        with self.SessionLocal() as db:
            result = models.Result(**asdict(object))
            db.add(result)
            db.commit()
            return db.query(models.Result).fitler(
                models.Result.id == result.id
            ).first()

    def get_task_by_id(self, id: int) -> models.Task:
        with self.SessionLocal() as db:
            return db.query(models.Task).filter(
                models.Task.id == id
            ).first()

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
            # db_logger.error(
            #     f"Error dumping database: code - {code}, command - {command}"
            # )
            sys.exit(10)


class MongoDB(DatabaseABC):

    def __init__(self) -> None:
        connect(
            db=envs.MONGO_DB,
            username=envs.MONGO_USER,
            password=envs.MONGO_PASSWORD,
            host=envs.MONGO_HOST,
            port=envs.MONGO_PORT
        )

    def get_car_by_vin(self, vin: str) -> Optional[mongo_models.Car]:
        return mongo_models.Car.objects(car_vin=vin).first()

    def bulk_save_cars(
        self,
        objects: List[Car]
    ) -> None:
        mongo_models.Car.objects.insert(
            [
                mongo_models.Car(
                    **asdict(car)
                )
                for car in objects
            ]
        )

    def bulk_save_tasks(
        self,
        objects: List[CreateTask]
    ) -> None:
        mongo_models.Task.objects.insert(
            [
                mongo_models.Task(
                    **asdict(task)
                )
                for task in objects
            ]
        )

    def bulk_save_results(
        self,
        objects: List[Result]
    ) -> None:
        mongo_models.Result.objects.insert(
            [
                mongo_models.Result(
                    **asdict(result)
                )
                for result in objects
            ]
        )

    def reset_tasks_status(self) -> None:
        mongo_models.Task.objects(
            in_work=True, completed=False
        ).update(in_work=False)

    def get_idle_tasks(self, limit: int) -> List[mongo_models.Task]:
        return mongo_models.Task.objects(
            in_work=False, completed=False
        ).limit(limit).all()

    def update_task(self, task: Task, **kwargs) -> mongo_models.Task:
        mongo_models.Task.objects(id=task.id).update_one(**kwargs)
        return mongo_models.Task.objects(id=task.id).first()

    def add_car(
        self,
        object: Car
    ) -> mongo_models.Car:
        car = mongo_models.Car(**asdict(object))
        car.save()
        return car

    def add_task(
        self,
        object: CreateTask
    ) -> mongo_models.Task:
        task = mongo_models.Task(**asdict(object))
        task.save()
        return task

    def add_result(
        self,
        object: Result
    ) -> mongo_models.Result:
        result = mongo_models.Result(**asdict(object))
        result.save()
        return result

    def get_task_by_id(self, id: int) -> mongo_models.Task:
        return mongo_models.Task.objects(id=id).first()


class DBInterface(DatabaseABC):
    def __init__(self, db_type: Literal["postgresql", "mongodb"]) -> None:
        self.db_type = db_type
        self.db: Optional[Union[PostgreSQL, MongoDB]] = get_db_class(db_type)()
        assert self.db is not None

    def get_car_by_vin(self, vin: str) -> Optional[models.Car]:
        return self.db.get_car_by_vin(vin)

    def bulk_save_cars(
        self,
        objects: List[Car]
    ) -> None:
        return self.db.bulk_save_cars(objects)

    def bulk_save_tasks(
        self,
        objects: List[CreateTask]
    ) -> None:
        return self.db.bulk_save_tasks(objects)

    def bulk_save_results(
        self,
        objects: List[Result]
    ) -> None:
        return self.db.bulk_save_results(objects)

    def reset_tasks_status(self) -> None:
        return self.db.reset_tasks_status()

    def get_idle_tasks(self, limit: int) -> List[models.Task]:
        return self.db.get_idle_tasks(limit)

    def update_task(self, task: models.Task, **kwargs) -> models.Task:
        return self.db.update_task(task, **kwargs)

    def add_car(
        self,
        object: Car
    ) -> mongo_models.Car:
        return self.db.add_car(object)

    def add_task(
        self,
        object: CreateTask
    ) -> mongo_models.Task:
        return self.db.add_task(object)

    def add_result(
        self,
        object: Result
    ) -> mongo_models.Result:
        return self.db.add_result(object)

    def get_task_by_id(self, id: int) -> models.Task:
        return self.db.get_task_by_id(id)
