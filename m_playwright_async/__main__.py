import asyncio
import requests
from urllib.parse import urljoin
from typing import List
from requests.exceptions import ChunkedEncodingError
from playwright.async_api import (
    async_playwright,
    Playwright,
    Browser,
    BrowserContext,
    Page
)

from parsers.parser import AutoriaParser, AutoriaParserV1, AutoriaParserV2
from utils.exceptions import (
    EmptyPageException,
    NoVinException,
    SoldException,
    NoUsernameException
)
from utils.log import get_logger
import envs
from database.dal import Car, DAL


BASE_URL = "https://auto.ria.com/uk/car/used/"
scraper_logger = get_logger("AutoriaScraper")


class AutoriaScraper:

    def __init__(self) -> None:
        self.results: List[Car] = []
        self.pages: int = envs.PAGES

        self.tasks: List[asyncio.Task] = []
        self.db_task: asyncio.Task = None
        self.max_tasks: int = 2

        self.db_is_busy: bool = False
        self.global_stop: bool = False

        self.playwright: Playwright = None
        self.browser: Browser = None
        self.context: BrowserContext = None

    def start_db(self) -> None:
        self.db_task = asyncio.create_task(self.bulk_save())

    async def bulk_save(self) -> None:
        while not self.global_stop:

            await asyncio.sleep(0.01)

            if not self.results:
                self.db_is_busy = False
                continue

            self.db_is_busy = True

            with DAL() as db:
                results: list = []
                for item in self.results[:]:
                    self.results.remove(item)
                    results.append(item)
                await asyncio.to_thread(db.process_items, results)

        scraper_logger.info("Database was shut down")

    async def start_playwright(self) -> None:
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=False,
        )
        self.context = await self.browser.new_context()
        scraper_logger.info("Started Playwright")

    async def stop_playwright(self) -> None:
        await self.context.close()
        await self.browser.close()
        await self.playwright.stop()
        scraper_logger.info("Playwright was shut down")

    def clean_tasks(self) -> None:
        for task in self.tasks:
            if task.done():
                try:
                    task.result()
                except EmptyPageException:
                    return True
                self.tasks.remove(task)

    async def accept_cookies(self) -> None:
        page = await self.context.new_page()
        await page.goto(BASE_URL)
        await page.get_by_text("Розумію і дозволяю").click()
        await page.close()

    async def run_task(self, page_number: int) -> None:
        page = await self.context.new_page()
        try:
            await self.scrape_list_page(page, page_number)
        finally:
            await page.close()

    async def scrape_list_page(self, page: Page, page_number: int) -> None:
        await page.goto(urljoin(BASE_URL, f"?page={page_number}"))

        content = await page.content()

        if not AutoriaParser.check_list_page(content):
            scraper_logger.warning("Reached last page. Terminating...")
            raise EmptyPageException("Reached last page.")

        urls = AutoriaParser.get_urls(content)

        scraper_logger.info(f"Parsing page {page_number}")

        for url in urls:
            await page.goto(url)

            if await page.query_selector(
                "//*[contains(@class, 'phone_show_link')]"
            ):
                content = await page.content()
                parser = AutoriaParserV1(content, url)
            else:
                print("Skipped page with new design:", url)
                print(
                    await page.query_selector(
                        "(//div[@class='sellerInfoHiddenPhone']"
                        "//button[@class='s1 conversion'])[1]"
                    )
                )
                continue

            try:
                self.results.append(parser.parse_detail_page())
            except (NoVinException, NoUsernameException, SoldException):
                continue

        scraper_logger.info(f"Finished parsing page {page_number}")

    async def run(self) -> None:
        current_page: int = 1

        scraper_logger.info("Launched parser")

        self.start_db()

        await self.start_playwright()
        await self.accept_cookies()

        try:
            while current_page <= self.pages:
                if self.clean_tasks():
                    break

                if len(self.tasks) < self.max_tasks:
                    task = asyncio.create_task(
                        self.run_task(current_page)
                    )
                    self.tasks.append(task)
                    current_page += 1
                    continue
                await asyncio.sleep(0.01)
        finally:
            await asyncio.gather(*self.tasks)

            self.global_stop = True

            while self.db_is_busy:
                await asyncio.sleep(0.01)

            await self.stop_playwright()

            scraper_logger.info("Finished parsing")

    @classmethod
    async def get_page(self, url: str) -> str:
        while True:
            try:
                response = requests.get(url, stream=False).text
            except ChunkedEncodingError:
                continue

            if AutoriaParser.validate(response):
                return response

            await asyncio.sleep(1)


async def main() -> None:
    scraper = AutoriaScraper()
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(main())
