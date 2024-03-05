from urllib.parse import urljoin
from typing import List, Literal
import asyncio
import aiohttp
from parsel import Selector

from database.dal import CarDAL
from parsers.parser import AutoriaParser, AutoriaParserV1
from utils.dto import Car
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

    def __init__(self, db_type: Literal["postgresql", "mongodb"]) -> None:
        self.results: List[Car] = []

        self.tasks: List[asyncio.Task] = []
        self.max_tasks = 100

        self.db: CarDAL = CarDAL(db_type)
        self.db_is_busy = False
        self.global_stop = False

        self.pages = envs.PAGES

    def start_db(self) -> None:
        self.db_task = asyncio.create_task(self.bulk_save())

    async def bulk_save(self) -> None:
        while not self.global_stop or self.db_is_busy:

            await asyncio.sleep(0.01)

            if not self.results:
                self.db_is_busy = False
                continue

            self.db_is_busy = True

            results = []
            for item in self.results[:]:
                self.results.remove(item)
                results.append(item)
            await asyncio.to_thread(self.db.process_items, results)

    def clean_tasks(self) -> None:
        for task in self.tasks:
            if task.done():
                try:
                    task.result()
                except EmptyPageException:
                    return True
                self.tasks.remove(task)

    async def get_list_car_data(
        self,
        page_number: int,
    ) -> None:
        page = await self.get_page(
            urljoin(BASE_URL, f"?page={page_number}")
        )

        scraper_logger.info(f"Parsing page {page_number}")

        urls = AutoriaParser.get_urls(page)

        for url in urls:
            detailed_page = await self.get_page(url)
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
                continue

        scraper_logger.info(f"Finished parsing page {page_number}")

    async def run(self) -> None:
        current_page: int = 1

        scraper_logger.info("Launched parser")

        self.start_db()

        try:
            while current_page <= self.pages:
                if self.clean_tasks():
                    break

                if len(self.tasks) < self.max_tasks:
                    task = asyncio.create_task(
                        self.get_list_car_data(current_page)
                    )
                    self.tasks.append(task)
                    current_page += 1
                    continue

                await asyncio.sleep(0.01)
        finally:
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
    scraper = AutoriaScraper("postgresql")
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(main())
