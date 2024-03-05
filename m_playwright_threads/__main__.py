from urllib.parse import urljoin
from typing import List, Literal
import threading
import time
from playwright.sync_api import (
    sync_playwright,
    Playwright,
    Browser,
    BrowserContext,
    Page
)
from parsel import Selector

from database.dal import CarDAL
from utils.dto import Car
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

    def __init__(self, db_type: Literal['postgresql', 'mongodb']) -> None:
        self.results: List[Car] = []
        self.pages: int = envs.PAGES

        self.threads: List[threading.Thread] = []
        self.max_threads: int = 4

        self.db_is_busy: bool = False
        self.db_thread: threading.Thread = None
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

            results: list = []
            for item in self.results[:]:
                self.results.remove(item)
                results.append(item)
            self.db.process_items(results)

    def clean_threads(self) -> None:
        for thread in self.threads:
            if not thread.is_alive():
                self.threads.remove(thread)

    def run_thread(self, page_number: int) -> None:
        playwright: Playwright = sync_playwright().start()
        browser: Browser = playwright.chromium.launch(
            headless=False,
        )
        context: BrowserContext = browser.new_context()
        page: Page = context.new_page()
        try:
            self.scrape_list_page(page, page_number)
        finally:
            page.close()
            context.close()
            browser.close()
            playwright.stop()

    def scrape_list_page(self, page: Page, page_number: int) -> None:
        page.goto(urljoin(BASE_URL, f"?page={page_number}"))
        content = page.content()

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
                selector = Selector(text=content)
                print(url)
                print(selector.xpath(
                    "//div[@class='sellerInfoHiddenPhone']"
                    "//button[@class='s1 conversion']"
                ))
                continue
            try:
                self.results.append(parser.parse_detail_page())
            except (NoVinException, NoUsernameException, SoldException):
                continue

        scraper_logger.info(f"Finished parsing page {page_number}")

    def run(self) -> None:
        current_page: int = 1

        scraper_logger.info("Launched parser")

        self.start_db_thread()

        try:
            while current_page <= self.pages:
                self.clean_threads()

                if len(self.threads) < self.max_threads:
                    thread: threading.Thread = threading.Thread(
                        target=self.run_thread,
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


if __name__ == "__main__":
    scraper = AutoriaScraper("postgresql")
    scraper.run()
