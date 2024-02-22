import queue
import requests
from urllib.parse import urljoin
from typing import List
import threading
import multiprocessing
import time
from requests.exceptions import ChunkedEncodingError

from database.db import PostgresDB
from parsers.parser import AutoriaParser
from utils.exceptions import (
    EmptyPageException,
    NoVinException,
    SoldException,
    NoUsernameException
)
from utils.log import LOGGER


BASE_URL = "https://auto.ria.com/uk/car/used/"


class AutoriaScraper:

    def __init__(self) -> None:
        self.processes: List[multiprocessing.Process] = []
        self.queue = multiprocessing.Queue()
        self.max_processes = 21
        self.pages = 150
        self.db_thread = threading.Thread(
            target=self.bulk_save,
            daemon=True,
        )
        self.db_thread.start()

    def bulk_save(self) -> None:
        while True:
            results = self.queue.get()
            with PostgresDB() as db:
                db.process_items(results)

    def clean_processes(self) -> None:
        for process in self.processes:
            if not process.is_alive():
                self.processes.remove(process)

    def run(self) -> None:
        current_page = 1

        LOGGER.info("Launched parser")

        try:
            while current_page <= self.pages:
                self.clean_processes()

                if len(self.processes) < self.max_processes:
                    process = multiprocessing.Process(
                        target=self.get_list_page_data,
                        args=(current_page, self.queue)
                    )
                    process.start()
                    self.processes.append(process)
                    current_page += 1
                    continue

                time.sleep(0.01)

        except EmptyPageException:
            pass

        for process in self.processes:
            process.join()

    @classmethod
    def get_page(cls, url: str) -> str:
        while True:
            try:
                response = requests.get(url, stream=False).text
            except ChunkedEncodingError:
                continue

            if AutoriaParser.validate(response):
                return response

            time.sleep(1)

    @classmethod
    def get_list_page_data(
        cls,
        page_number: int,
        queue: multiprocessing.Queue
    ) -> None:
        page = cls.get_page(
            urljoin(BASE_URL, f"?page={page_number}")
        )

        LOGGER.info(f"Parsing page {page_number}")

        urls = AutoriaParser.get_urls(page)
        cars_number = len(urls)
        results = []
        for url in urls:
            detailed_page = cls.get_page(url)
            try:
                parser = AutoriaParser(detailed_page, url)
                results.append(
                    parser.parse_detail_page()
                )
            except (SoldException, NoVinException, NoUsernameException):
                cars_number -= 1
                continue

        queue.put(
            results
        )

        LOGGER.info(
            f"Finished parsing page {page_number}. Cars number: {cars_number}."
        )
