from parsel import Selector
import requests
from urllib.parse import urljoin
from typing import List, Literal
import threading
import multiprocessing
import time
from requests.exceptions import ChunkedEncodingError

from database.dal import CarDAL
from parsers.parser import AutoriaParser, AutoriaParserV1
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


class Scraper:
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
        results = []
        for url in urls:
            detailed_page = self.get_page(url)
            page = Selector(text=detailed_page)

            if not page.xpath("//*[contains(@class, 'phone_show_link')]"):
                scraper_logger.info("Skipped page with new design.")
                continue
            else:
                parser = AutoriaParserV1(detailed_page, url)

            try:
                results.append(
                    parser.parse_detail_page()
                )
            except (SoldException, NoVinException, NoUsernameException):
                continue

        self.queue.put(
            results
        )

        scraper_logger.info(
            f"Finished parsing page {page_number}"
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


class AutoriaScraper:

    def __init__(self, db_type: Literal['postgresql', 'mongodb']) -> None:
        self.processes: List[multiprocessing.Process] = []
        self.queue: multiprocessing.Queue = multiprocessing.Queue()
        self.max_processes: int = 21

        self.db: CarDAL = CarDAL(db_type)

        self.pages: int = envs.PAGES

    def run_db_thread(self) -> None:
        self.db_thread: threading.Thread = threading.Thread(
            target=self.bulk_save,
            daemon=True,
        )
        self.db_thread.start()

    def bulk_save(self) -> None:
        while True:
            results = self.queue.get()
            self.db.process_items(results)

    def clean_processes(self) -> None:
        for process in self.processes:
            if not process.is_alive():
                self.processes.remove(process)

    def run(self) -> None:
        current_page: int = 1

        scraper_logger.info("Launched parser")

        self.run_db_thread()

        scraper = Scraper(self.queue)

        try:
            while current_page <= self.pages:
                self.clean_processes()

                if len(self.processes) < self.max_processes:
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
        finally:
            for process in self.processes:
                process.join()

            scraper_logger.info("Finished parsing")
