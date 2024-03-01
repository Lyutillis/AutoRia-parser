import asyncio
from typing import List
import json
from dataclasses import asdict
from dacite import from_dict
from playwright.async_api import (
    async_playwright,
    Playwright,
    Browser,
    BrowserContext,
    Page
)
from urllib.parse import urljoin
from datetime import datetime

import redis

from database.dal import DAL
from utils.dto import Car, Task, Result
from utils.cache import AsyncCache
from utils.log import get_logger
from utils.exceptions import (
    EmptyPageException,
    NoVinException,
    SoldException,
    NoUsernameException
)
from parsers.parser import AutoriaParser, AutoriaParserV1, AutoriaParserV2
from utils.encoders import DateTimeEncoder


BASE_URL = "https://auto.ria.com/uk/car/used/"
worker_logger = get_logger("Worker")


class Worker:
    def __init__(self) -> None:
        self.results: List[Result] = []
        self.cache_0: AsyncCache = AsyncCache(0)

        self.asyncio_tasks: List[asyncio.Task] = []
        self.max_tasks: int = 2

        self.playwright: Playwright = None
        self.browser: Browser = None
        self.context: BrowserContext = None

    async def get_task(self) -> None:
        while True:
            task = await self.cache_0.red.rpop("tasks_queue")
            if task:
                return Task(
                    **json.loads(task)
                )
            await asyncio.sleep(5)

    async def save_results(self) -> None:
        for result in self.results[:]:
            await self.cache_0.red.lpush(
                "results_queue",
                json.dumps(asdict(result), cls=DateTimeEncoder)
            )
            self.results.remove(result)

    async def start_playwright(self) -> None:
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=False,
        )
        self.context = await self.browser.new_context()
        worker_logger.info("Started Playwright")

    async def stop_playwright(self) -> None:
        await self.context.close()
        await self.browser.close()
        await self.playwright.stop()
        worker_logger.info("Playwright was shut down")

    def clean_asyncio_tasks(self) -> None:
        for task in self.asyncio_tasks:
            if task.done():
                self.asyncio_tasks.remove(task)

    async def accept_cookies(self) -> None:
        page = await self.context.new_page()
        await page.goto(BASE_URL)
        await page.get_by_text("Розумію і дозволяю").click()
        await page.close()

    async def process_page(self, page: Page, task: Task) -> None:
        print(asdict(task))
        page_number = task.page_number
        await page.goto(urljoin(BASE_URL, f"?page={page_number}"))

        content = await page.content()

        if not AutoriaParser.check_list_page(content):
            worker_logger.warning("Got empty page. Skipping...")
            raise EmptyPageException("Reached empty page.")

        urls = AutoriaParser.get_urls(content)

        worker_logger.info(f"Parsing page {page_number}")

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
                self.results.append(
                    Result(
                        task.id,
                        parser.parse_detail_page()
                    )
                )
            except (NoVinException, NoUsernameException, SoldException):
                continue

        worker_logger.info(f"Finished parsing page {page_number}")

    async def run_asyncio_task(self, task: Task) -> None:
        print(1)
        page = await self.context.new_page()
        print(2)
        try:
            print(3)
            await self.process_page(page, task)
        finally:
            await page.close()

    async def run(self) -> None:
        await self.start_playwright()
        await self.accept_cookies()
        try:
            while True:
                print(4)
                self.clean_asyncio_tasks()

                if len(self.asyncio_tasks) < self.max_tasks:
                    task = await self.get_task()
                    self.asyncio_tasks.append(
                        asyncio.create_task(
                            self.run_asyncio_task(task)
                        )
                    )

                await self.save_results()
        finally:
            await asyncio.gather(*self.asyncio_tasks)

            await self.stop_playwright()


async def main() -> None:
    worker = Worker()
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
