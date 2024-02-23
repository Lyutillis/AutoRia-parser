from urllib.parse import urljoin
from typing import List
import asyncio
import aiohttp

from database.db_layer import PostgresDB, Car
from parsers.parser import AutoriaParser
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

    def __init__(self) -> None:
        self.results: List[Car] = []
        self.tasks: List[asyncio.Task] = []
        self.max_tasks = 100
        self.db_is_busy = False
        self.pages = envs.PAGES
        self.global_stop = False
        self.tasks.append(
            asyncio.create_task(self.bulk_save())
        )

    async def bulk_save(self) -> None:
        while not self.global_stop or self.db_is_busy:

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
                await asyncio.to_thread(db.process_items, results)

    def clean_tasks(self) -> None:
        for task in self.tasks:
            if not task.done():
                self.tasks.remove(task)

    async def get_list_car_data(
        self,
        url: int,
    ) -> None:
        detailed_page = await self.get_page(url)
        try:
            parser = AutoriaParser(detailed_page, url)
            self.results.append(
                parser.parse_detail_page()
            )
        except (SoldException, NoVinException, NoUsernameException):
            pass

    async def run(self) -> None:
        current_page = 1
        scraper_logger.info("Launched parser")

        while current_page <= self.pages:
            self.clean_tasks()

            if len(self.tasks) < self.max_tasks:
                page = await self.get_page(
                    urljoin(BASE_URL, f"?page={current_page}")
                )
                if not AutoriaParser.check_list_page(page):
                    scraper_logger.warning("Reached last page. Terminating...")
                    break

                scraper_logger.info(f"Parsing page {current_page}")

                urls = AutoriaParser.get_urls(page)

                for url in urls:

                    task = asyncio.create_task(
                        self.get_list_car_data(url)
                    )
                    self.tasks.append(task)

                current_page += 1
                continue

            await asyncio.sleep(0.01)

        await asyncio.gather(*self.tasks)

        self.global_stop = True

        while self.db_is_busy:
            await asyncio.sleep(0.1)

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


async def main() -> None:
    scraper = AutoriaScraper()
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(main())
