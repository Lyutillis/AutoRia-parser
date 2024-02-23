import requests
from urllib.parse import urljoin
from typing import List
import threading
import time
from requests.exceptions import ChunkedEncodingError
import asyncio
import aiohttp
from contextlib import suppress

from database.db_layer import PostgresDB, Car
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
scraper_logger = get_logger("AutoriaScraper")


class AutoriaScraper:

    def __init__(self) -> None:
        self.results: List[Car] = []
        self.tasks: List[asyncio.Task] = []
        self.db_is_busy = False
        self.pages = envs.PAGES

    async def bulk_save(self) -> None:
        while True:

            await asyncio.sleep(0.01)
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

    # def clean_threads(self) -> None:
    #     for thread in self.threads:
    #         if not thread.is_alive():
    #             self.threads.remove(thread)

    async def get_list_page_data(
        self,
        page_number: int,
    ) -> None:
        page = await self.get_page(
            urljoin(BASE_URL, f"?page={page_number}")
        )

        scraper_logger.info(f"Parsing page {page_number}")

        urls = AutoriaParser.get_urls(page)
        cars_number = len(urls)
        for url in urls:
            detailed_page = await self.get_page(url)
            try:
                parser = AutoriaParser(detailed_page, url)
                self.results.append(
                    parser.parse_detail_page()
                )
            except (SoldException, NoVinException, NoUsernameException):
                cars_number -= 1
                continue

        scraper_logger.info(
            f"Finished parsing page {page_number}. Cars number: {cars_number}."
        )

    async def main(self) -> None:
        current_page = 1
        db_task = asyncio.create_task(
            self.bulk_save()
        )
        scraper_logger.info("Launched parser")

        try:
            while current_page <= self.pages:
                # await self.clean_threads()

                # if len(self.threads) < self.max_threads:
                task = asyncio.create_task(
                    self.get_list_page_data(current_page)
                )
                self.tasks.append(task)
                current_page += 1
                # continue

                # time.sleep(0.01)

        except EmptyPageException:
            pass

        await asyncio.gather(*self.tasks)

        while self.db_is_busy:
            await asyncio.sleep(0.1)

        db_task.cancel()
        with suppress(asyncio.CancelledError):
            asyncio.run(db_task)

        scraper_logger.info("Finished parsing")

    @classmethod
    async def get_page(self, url: str) -> str:
        while True:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    html = await response.text()

                    if AutoriaParser.validate(html):
                        return html

            await asyncio.sleep(1)


if __name__ == "__main__":
    scraper = AutoriaScraper()
    asyncio.run(scraper.main())
