import requests
from urllib.parse import urljoin
from typing import List
import threading
import time
import playwright
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

    def bulk_save(self) -> None:
        with PostgresDB() as db:
            db.process_items(self.results)
            self.results = []

    def accept_cookies(self, page):
        page.get_by_text("Розумію і дозволяю").click()

    def run(self) -> None:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=False,
            )
            context = browser.new_context()
            page = context.new_page()

            current_page = 1

            scraper_logger.info("Launched parser")

            page.goto(BASE_URL)
            self.accept_cookies(page)

            # page.wait_for_selector(
            #     "//*[contains(@class, ['app-header'])]"
            #     "//*[contains(@class, 'social-section-wrapper')]",
            # )

            while current_page <= self.pages:
                page.goto(urljoin(BASE_URL, f"?page={current_page}"))
                content = page.content()

                if not AutoriaParser.check_list_page(content):
                    scraper_logger.warning("Reached last page. Terminating...")
                    break

                urls = AutoriaParser.get_urls(content)

                for url in urls:
                    page.goto(url)
                    content = page.content()

                    if page.query_selector(
                        "//*[contains(@class, 'phone_show_link')]"
                    ):
                        page.locator(
                            "//*[contains(@class, 'phone_show_link')]"
                        ).click()
                        parser = AutoriaParserV1(content, url)
                    else:
                        page.locator(
                            "//*[contains(@data-action, 'showBottomPopUp')]"
                        ).click()
                        parser = AutoriaParserV2(content, url)

                    self.results.append(parser.parse_detail_page())

                self.bulk_save()

                current_page += 1

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
