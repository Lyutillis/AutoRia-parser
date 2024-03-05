from urllib.parse import urljoin
from typing import List, Literal
from playwright.sync_api import sync_playwright

from database.dal import CarDAL
from parsers.parser import AutoriaParser, AutoriaParserV1, AutoriaParserV2
from utils.dto import Car
from utils.exceptions import (
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
        self.db: CarDAL = CarDAL(db_type)

    def bulk_save(self) -> None:
        self.db.process_items(self.results)
        self.results = []

    def scrape_list_page(self, page, page_number: int) -> None:
        page.goto(urljoin(BASE_URL, f"?page={page_number}"))
        content = page.content()

        if not AutoriaParser.check_list_page(content):
            scraper_logger.warning("Reached last page. Terminating...")
            return True

        urls = AutoriaParser.get_urls(content)
        scraper_logger.info(f"Parsing page {page_number}")
        for url in urls:
            page.goto(url)
            content = page.content()

            if page.query_selector(
                "//*[contains(@class, 'phone_show_link')]"
            ):
                content = page.content()
                parser = AutoriaParserV1(content, url)
            else:
                print("Skipped page with new design:", url)
                print(
                    page.query_selector(
                        "//div[@id='sellerInfoHiddenPhone']"
                    )
                )
                continue

            try:
                self.results.append(parser.parse_detail_page())
            except (NoVinException, NoUsernameException, SoldException):
                continue

        self.bulk_save()
        scraper_logger.info(f"Finished parsing page {page_number}")

    def run(self) -> None:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=False,
            )
            context = browser.new_context()
            page = context.new_page()

            current_page = 1

            scraper_logger.info("Launched parser")

            while current_page <= self.pages:
                if self.scrape_list_page(page, current_page):
                    break

                current_page += 1

        scraper_logger.info("Finished parsing")


if __name__ == "__main__":
    scraper = AutoriaScraper("postgresql")
    scraper.run()
