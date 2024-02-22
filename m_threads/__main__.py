import requests
from urllib.parse import urljoin
from typing import List
import threading
import time
from requests.exceptions import ChunkedEncodingError

from database.db import PostgresDB, Car
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
        self.results: List[Car] = []
        self.threads: List[threading.Thread] = []
        self.db_is_busy = False
        self.max_threads = 21
        self.pages = 150
        self.db_thread = threading.Thread(
            target=self.bulk_save,
            daemon=True,
        )
        self.db_thread.start()

    def bulk_save(self) -> None:
        while True:

            time.sleep(0.01)
            if not self.results:
                self.db_is_busy = False
                continue

            self.db_is_busy = True

            with PostgresDB() as db:
                results = []
                for item in self.results[:]:
                    self.results.remove(item)
                    results.append(item)
                db.process_items(results)

    def clean_threads(self) -> None:
        for thread in self.threads:
            if not thread.is_alive():
                self.threads.remove(thread)

    def get_list_page_data(
        self,
        page_number: int,
    ) -> None:
        page = self.get_page(
            urljoin(BASE_URL, f"?page={page_number}")
        )

        LOGGER.info(f"Parsing page {page_number}")

        urls = AutoriaParser.get_urls(page)
        cars_number = len(urls)
        for url in urls:
            detailed_page = self.get_page(url)
            try:
                parser = AutoriaParser(detailed_page, url)
                self.results.append(
                    parser.parse_detail_page()
                )
            except (SoldException, NoVinException, NoUsernameException):
                cars_number -= 1
                continue

        LOGGER.info(
            f"Finished parsing page {page_number}. Cars number: {cars_number}."
        )

    def run(self) -> None:
        current_page = 1

        LOGGER.info("Launched parser")

        try:
            while current_page <= self.pages:
                self.clean_threads()

                if len(self.threads) < self.max_threads:
                    thread = threading.Thread(
                        target=self.get_list_page_data,
                        args=(current_page,)
                    )
                    thread.start()
                    self.threads.append(thread)
                    current_page += 1
                    continue

                time.sleep(0.01)

        except EmptyPageException:
            pass

        for thread in self.threads:
            thread.join()

        while self.db_is_busy:
            time.sleep(0.1)

    @classmethod
    def get_page(self, url: str) -> str:
        while True:
            try:
                response = requests.get(url, stream=False).text
            except ChunkedEncodingError:
                continue

            if AutoriaParser.validate(response):
                return response

            time.sleep(1)


if __name__ == "__main__":
    scraper = AutoriaScraper()
    scraper.run()
