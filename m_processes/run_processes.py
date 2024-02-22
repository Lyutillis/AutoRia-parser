import requests
from urllib.parse import urljoin
from typing import List
import threading
import multiprocessing
import time
from requests.exceptions import ChunkedEncodingError

from database.db_layer import PostgresDB
from parsers.parser import AutoriaParser
from utils.exceptions import (
    EmptyPageException,
    NoVinException,
    SoldException,
    NoUsernameException
)
from utils.log import get_logger
import envs


BASE_URL = "https://auto.ria.com/uk/car/used/"

scraper_logger = get_logger("Scraper")
runner_logger = get_logger("Runner")


class AutoriaScraper:

    def __init__(self, queue: multiprocessing.Queue) -> None:
        self.queue = queue

    def get_list_page_data(
        self,
        page_number: int
    ) -> None:
        page = self.get_page(
            urljoin(BASE_URL, f"?page={page_number}")
        )

        scraper_logger.info(f"Parsing page {page_number}")

        urls = AutoriaParser.get_urls(page)
        cars_number = len(urls)
        results = []
        for url in urls:
            detailed_page = self.get_page(url)
            try:
                parser = AutoriaParser(detailed_page, url)
                results.append(
                    parser.parse_detail_page()
                )
            except (SoldException, NoVinException, NoUsernameException):
                cars_number -= 1
                continue

        self.queue.put(
            results
        )

        scraper_logger.info(
            f"Finished parsing page {page_number}. Cars number: {cars_number}."
        )

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


class ProcessRunner:

    def __init__(self) -> None:
        self.processes: List[multiprocessing.Process] = []
        self.queue = multiprocessing.Queue()
        self.max_processes = 21
        self.pages = envs.PAGES
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

        runner_logger.info("Launched parser")

        try:
            while current_page <= self.pages:
                self.clean_processes()

                if len(self.processes) < self.max_processes:
                    scraper = AutoriaScraper(self.queue)
                    process = multiprocessing.Process(
                        target=scraper.get_list_page_data,
                        args=(current_page,)
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

        runner_logger.info("Finished parsing")
