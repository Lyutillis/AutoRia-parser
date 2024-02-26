import requests
from urllib.parse import urljoin
from typing import List
import threading
import time
from requests.exceptions import ChunkedEncodingError
from playwright.sync_api import sync_playwright

from database.db_layer import PostgresDB, Car
from parsers.parser import AutoriaParser, AutoriaParserV1, AutoriaParserV2
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

    def __init__(self) -> None:
        self.results: List[Car] = []
        self.pages = envs.PAGES

        self.threads: List[threading.Thread] = []
        self.db_is_busy = False
        self.max_threads = 4
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

    def accept_cookies(self, page):
        page.get_by_text("Розумію і дозволяю").click()

    def scrape_list_page(self, page_number: int) -> None:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=False,
            )
            context = browser.new_context()
            page = context.new_page()

            page.goto(urljoin(BASE_URL, f"?page={page_number}"))
            content = page.content()

            self.accept_cookies(page)

            if not AutoriaParser.check_list_page(content):
                scraper_logger.warning("Reached last page. Terminating...")
                raise EmptyPageException("Reached last page.")

            urls = AutoriaParser.get_urls(content)
            scraper_logger.info(f"Parsing page {page_number}")
            for url in urls:
                page.goto(url)

                if page.query_selector(
                    "//*[contains(@class, 'phone_show_link')]"
                ):
                    page.locator(
                        "(//*[contains(@class, 'phone_show_link')])[1]"
                    ).click()
                    time.sleep(0.3)
                    content = page.content()
                    parser = AutoriaParserV1(content, url)
                else:
                    content = page.content()
                    from parsel import Selector
                    selector = Selector(text=content)
                    print(selector.xpath(
                        "//div[@class='sellerInfoHiddenPhone']"
                        "//button[@class='s1 conversion')]"
                    ))
                    # page.locator(
                    #     "(//div[@class='sellerInfoHiddenPhone']"
                    #     "//button[@class='s1 conversion')])[1]"
                    # ).click()
                    # time.sleep(0.3)
                    # content = page.content()
                    # parser = AutoriaParserV2(content, url)

                try:
                    self.results.append(parser.parse_detail_page())
                except (NoVinException, NoUsernameException, SoldException):
                    continue

        scraper_logger.info(f"Finished parsing page {page_number}")

    def run(self) -> None:
        current_page = 1

        scraper_logger.info("Launched parser")

        try:
            while current_page <= self.pages:
                self.clean_threads()

                if len(self.threads) < self.max_threads:
                    thread = threading.Thread(
                        target=self.scrape_list_page,
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
    scraper = AutoriaScraper()
    scraper.run()
