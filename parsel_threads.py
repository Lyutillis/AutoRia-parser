from ast import Attribute
from pprint import pprint
import requests
from urllib.parse import urljoin
from datetime import datetime
from typing import List, Tuple
from parsel import Selector
import json
import threading
import multiprocessing
import time
from requests.exceptions import ChunkedEncodingError

from app.db import PostgresDB, Car
from app.exceptions import (
    EmptyPageException,
    NoVinException,
    SoldException,
    NoUsernameException
)
from app.log import LOGGER


BASE_URL = "https://auto.ria.com/uk/car/used/"
PHONE_URL = "https://auto.ria.com/users/phones/"


class AutoriaParser:
    def __init__(self, html: str, url: str) -> None:
        self.html = Selector(text=html)
        self.url = url

    @classmethod
    def get_urls(cls, html: str) -> str:
        page = Selector(text=html)
        return page.xpath(
            (
                "//*[contains(@class, 'content-bar')]"
                "//a[contains(@class, 'm-link-ticket')]/@href"
            )
        ).getall()

    def get_title(self) -> str:
        return self.html.xpath(
            "//h1[contains(@class, 'head')]//@title"
        ).get()

    def get_price_usd(self) -> float:
        price_usd = self.html.xpath(
            "//div[contains(@class, 'price_value')]//strong/text()"
        ).get()
        for char in ("$", "грн", "€"):
            price_usd = price_usd.replace(char, "")
        return float(price_usd.strip().replace(" ", ""))

    def get_odometer(self) -> float:
        odometer = self.html.xpath(
            "//div[contains(@class, 'base-information')]//span/text()"
        ).get()
        return float(odometer)

    def get_username(self) -> str:
        attempts = 3
        while attempts:
            attempts -= 1
            try:
                username = (
                    self.html.xpath(
                        "//*[contains(@class, 'seller_info_name')]//a/text()"
                    ).get(),
                    self.html.xpath(
                        "//*[contains(@class, 'seller_info_name')]/text()"
                    ).get()
                )
                if username[0]:
                    return username[0].strip()
                elif username[1]:
                    return username[1].strip()

            except AttributeError:
                time.sleep(4)
        raise NoUsernameException("Unable to parse the username!")

    def get_phone_number(self) -> str:
        user_id = self.html.xpath("//body/@data-auto-id").get()
        user_hash = self.html.xpath(
            "//*[starts-with(@class, 'js-user-secure-')]/@data-hash"
        ).get()
        expires = self.html.xpath(
            "//*[starts-with(@class, 'js-user-secure-')]/@data-expires"
        ).get()

        phone_url = urljoin(
            PHONE_URL, f"{user_id}?hash={user_hash}&expires={expires}"
        )
        return "+38" + json.loads(
            requests.get(phone_url, stream=False).text
        )["formattedPhoneNumber"]

    def get_image_url(self) -> str:
        return self.html.xpath(
            (
                "//div[contains(@class, 'carousel-inner')]"
                "//div//source/@srcset"
            )
        ).get()

    def get_images_count(self) -> int:
        try:
            return int(
                self.html.xpath(
                    (
                        "//span[contains(@class, 'count')]"
                        "//*[contains(@class, 'mhide')]/text()"
                    )
                ).get().split()[1]
            )
        except AttributeError:
            return 0

    def get_car_number(self) -> str:
        car_number = self.html.xpath(
            "//*[contains(@class, 'state-num')]/text()"
        ).get()
        if car_number:
            car_number = car_number.strip()
        return car_number

    def get_car_vin(self) -> str:
        car_vin = (
            self.html.xpath(
                "//span[contains(@class, 'label-vin')]//text()"
            ).get(),
            self.html.xpath(
                "//span[contains(@class, 'vin-code')]/text()"
            ).get(),
        )

        if car_vin[0]:
            return car_vin[0].strip()
        elif car_vin[1]:
            return car_vin[1].strip()

        raise NoVinException("Vehicle doesnt have vin-code")

    def parse_detail_page(self) -> Car:

        if self.html.xpath("//*[contains(@class, 'icon-sold-out')]"):
            raise SoldException("Vehicle already sold!")

        return Car(
            url=self.url,
            title=self.get_title(),
            price_usd=self.get_price_usd(),
            odometer=self.get_odometer(),
            username=self.get_username(),
            phone_number=self.get_phone_number(),
            image_url=self.get_image_url(),
            images_count=self.get_images_count(),
            car_number=self.get_car_number(),
            car_vin=self.get_car_vin(),
            datetime_found=datetime.now()
        )

    @classmethod
    def validate(cls, html: str):
        page = Selector(text=html)

        if (
            page.xpath("//*[contains(@class, 'app-head')]")
            and page.xpath("//*[contains(@class, 'footer-line-wrap')]")
        ):
            return True
        elif not page.xpath("//*[contains(@class, 'ticket-item ')]"):
            raise EmptyPageException("This page is empty!")
        return False


class AutoriaScraper:

    def __init__(self, results: List = []) -> None:
        self.results = results

    def get_page(self, url: str) -> str:
        while True:
            try:
                response = requests.get(url, stream=False).text
            except ChunkedEncodingError:
                continue

            if AutoriaParser.validate(response):
                return response

            time.sleep(1)

    def get_list_page_data(self, page_number: int) -> None:
        page = self.get_page(
            urljoin(BASE_URL, f"?page={page_number}")
        )

        LOGGER.info(f"Parsing page {page_number}")

        urls = AutoriaParser.get_urls(page)
        cars_number = len(urls)
        for url in urls:
            detailed_page = self.get_page(url)
            try:
                parser = AutoriaParser(detailed_page, url)
                self.results.append(
                    parser.parse_detail_page()
                )
            except (SoldException, NoVinException, NoUsernameException):
                cars_number -= 1
                continue
        LOGGER.info(
            f"Finished parsing page {page_number}. Cars number: {cars_number}."
        )


class Main:

    def __init__(self) -> None:
        self.results: List[Car] = []
        self.threads: List[threading.Thread] = []
        self.db_is_busy = False
        self.max_threads = 21
        self.pages = 150
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

    def run(self) -> None:
        current_page = 1

        LOGGER.info("Launched parser")

        try:
            while current_page <= self.pages:
                self.clean_threads()

                if len(self.threads) < self.max_threads:
                    scraper = AutoriaScraper(self.results)
                    thread = threading.Thread(
                        target=scraper.get_list_page_data,
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


if __name__ == "__main__":
    parser = Main()
    parser.run()
