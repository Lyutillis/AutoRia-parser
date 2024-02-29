from typing import List
import json
from dataclasses import asdict
from dacite import from_dict
import time
from datetime import datetime

from database.dal import CarDAL, TaskDAL, ResultDAL
from utils.dto import Car, Task, Result
from utils.cache import Cache
from utils.log import get_logger


orchestrator_logger = get_logger("Orchestrator")


class Orchestrator:
    def __init__(self) -> None:
        self.tasks: List[Task] = []
        self.results: List[Result] = []
        self.cache_0: Cache = Cache(0)

    def create_tasks(self) -> None:
        with TaskDAL() as db:
            db.create_tasks()

    def reset_tasks_status(self) -> None:
        with TaskDAL() as db:
            db.reset_tasks_status()

    def get_tasks(self, count: int = 10) -> None:
        with TaskDAL() as db:
            self.tasks.extend(db.get_tasks(count))

    def pass_tasks(self) -> None:
        for task in self.tasks:
            self.cache_0.red.lpush(
                "tasks_queue",
                json.dumps(asdict(task))
            )
        self.tasks = []

    def get_results(self) -> None:
        redis_result = self.cache_0.red.rpop("results_queue")
        while redis_result:
            data = json.loads(redis_result)
            result = Result(
                task_id=data.pop("task_id"),
                car=Car(
                    **data["car"]
                )
            )
            self.results.append(result)
            redis_result = self.cache_0.red.rpop("results_queue")

    def save_results(self) -> None:
        if self.results:
            with ResultDAL() as db:
                db.save_results(self.results)
            self.results = []

    def run(self) -> None:
        self.create_tasks()
        self.reset_tasks_status()

        while True:
            self.get_tasks()
            self.pass_tasks()
            self.get_results()
            self.save_results()
            time.sleep(15)


if __name__ == "__main__":
    orchestrator = Orchestrator()
    orchestrator.run()
