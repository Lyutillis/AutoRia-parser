from typing import List, Literal
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
    def __init__(self, db_type: Literal["postgresql", "mongodb"]) -> None:
        self.tasks: List[Task] = []
        self.results: List[Result] = []
        self.cache_0: Cache = Cache(0)

        self.task_dal: TaskDAL = TaskDAL(db_type)
        self.result_dal: ResultDAL = ResultDAL(db_type)

    def create_tasks(self) -> None:
        self.task_dal.create_tasks()

    def reset_tasks_status(self) -> None:
        self.task_dal.reset_tasks_status()

    def get_tasks(self, count: int = 10) -> None:
        self.tasks.extend(
            self.task_dal.get_tasks(count)
        )

    def pass_tasks(self) -> None:
        for task in self.tasks:
            self.cache_0.red.lpush(
                "tasks_queue",
                json.dumps(asdict(task))
            )
        self.tasks = []

    def get_results(self) -> None:
        orchestrator_logger.info("Getting results from Redis")
        while True:
            item = self.cache_0.red.rpop("results_queue")
            if not item:
                return
            data = json.loads(item)
            result = Result(
                task_id=data.pop("task_id"),
                car=Car(
                    **data["car"]
                )
            )
            self.results.append(result)

    def save_results(self) -> None:
        if self.results:
            self.result_dal.save_results(self.results)
            self.results = []

    def run(self) -> None:
        self.reset_tasks_status()

        while True:
            self.get_tasks()
            self.pass_tasks()
            self.get_results()
            self.save_results()
            time.sleep(20)


if __name__ == "__main__":
    orchestrator = Orchestrator("postgresql")
    orchestrator.run()
