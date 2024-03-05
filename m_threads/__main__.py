from parsel import Selector
import requests
from urllib.parse import urljoin
from typing import List, Literal
import threading
import time
from requests.exceptions import ChunkedEncodingError

from database.dal import CarDAL
from utils.dto import Car
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
scraper_logger = get_logger("AutoriaScraper")


class AutoriaScraper:

    def __init__(self, db_type: Literal['postgresql', 'mongodb']) -> None:
        self.results: List[Car] = []
        self.pages = envs.PAGES

        self.threads: List[threading.Thread] = []
        self.max_threads = 21

        self.db_is_busy = False
        self.db: CarDAL = CarDAL(db_type)

    def start_db_thread(self) -> None:
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

            results = []
            for item in self.results[:]:
                self.results.remove(item)
                results.append(item)
            self.db.process_items(results)

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

        scraper_logger.info(f"Parsing page {page_number}")

        urls = AutoriaParser.get_urls(page)
        cars_number = len(urls)
        for url in urls:
            detailed_page = self.get_page(url)
            page = Selector(text=detailed_page)

            if not page.xpath("//*[contains(@class, 'phone_show_link')]"):
                scraper_logger.info("Skipped page with new design.")
                continue
            else:
                parser = AutoriaParserV1(detailed_page, url)

            try:
                self.results.append(
                    parser.parse_detail_page()
                )
            except (SoldException, NoVinException, NoUsernameException):
                cars_number -= 1
                continue

        scraper_logger.info(
            f"Finished parsing page {page_number}. Cars number: {cars_number}."
        )

    def run(self) -> None:
        current_page = 1

        scraper_logger.info("Launched parser")

        self.start_db_thread()

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
        finally:
            for thread in self.threads:
                thread.join()

            while self.db_is_busy:
                time.sleep(0.1)

            scraper_logger.info("Finished parsing")

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
    scraper = AutoriaScraper("postgresql")
    scraper.run()
