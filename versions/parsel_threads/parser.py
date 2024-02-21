from ast import Attribute
from pprint import pprint
import requests
from urllib.parse import urljoin
from datetime import datetime
from typing import List, Tuple
from parsel import Selector
import json
import threading
import time

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


class CarParser:
    def __init__(self, results: List) -> None:
        self.results = results

    def get_page(self, url: str) -> Selector:
        while True:
            response = requests.get(url, stream=False).text
            page = Selector(text=response)

            if (
                page.xpath("//*[contains(@class, 'app-head')]")
                and page.xpath("//*[contains(@class, 'footer-line-wrap')]")
            ):
                return page

            time.sleep(1)

    def get_url(self, car: Selector) -> str:
        return car.xpath(
            (
                "//*[contains(@class, 'content-bar')]"
                "//a[contains(@class, 'm-link-ticket')]/@href"
            )
        ).get()

    def get_title(self, car: Selector) -> str:
        return car.xpath(
            "//*[contains(@class, 'ticket-title')]//a/@title"
        ).get()

    def get_price_usd(self, car: Selector) -> float:
        price_usd = car.xpath(
            "//*[contains(@class, 'price-ticket')]/@data-main-price"
        ).get()
        return float(price_usd)

    def get_odometer(self, car: Selector) -> float:
        odometer = car.xpath("//*[contains(@class, 'js-race')]/text()").get()
        return float(odometer.split()[0])

    def get_username(self, car: Selector) -> str:
        attempts = 3
        while attempts:
            attempts -= 1
            try:
                username = (
                    car.xpath(
                        "//*[contains(@class, 'seller_info_name')]//a/text()"
                    ).get(),
                    car.xpath(
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

    def get_phone_number(self, car: Selector) -> str:
        user_id = car.xpath("//body/@data-auto-id").get()
        user_hash = car.xpath(
            "//*[starts-with(@class, 'js-user-secure-')]/@data-hash"
        ).get()
        expires = car.xpath(
            "//*[starts-with(@class, 'js-user-secure-')]/@data-expires"
        ).get()

        phone_url = urljoin(
            PHONE_URL, f"{user_id}?hash={user_hash}&expires={expires}"
        )
        return "+38" + json.loads(
            requests.get(phone_url, stream=False).text
        )["formattedPhoneNumber"]

    def get_image_url(self, car: Selector) -> str:
        return car.xpath("//img[contains(@class, 'outline')]/@src").get()

    def get_images_count(self, car: Selector) -> int:
        try:
            return int(
                car.xpath(
                    (
                        "//span[contains(@class, 'count')]"
                        "//*[contains(@class, 'mhide')]/text()"
                    )
                ).get().split()[1]
            )
        except AttributeError:
            return 0

    def get_car_number(self, car: Selector) -> str:
        car_number = car.xpath(
            "//*[contains(@class, 'state-num')]/text()"
        ).get()
        if car_number:
            car_number = car_number.strip()
        return car_number

    def get_car_vin(self, car: Selector) -> str:
        car_vin = (
            car.xpath("//*[contains(@class, 'label-vin')]//span/text()").get(),
            car.xpath("//*[contains(@class, 'vin-code')]/text()").get(),
        )

        if car_vin[0]:
            return car_vin[0].strip()
        elif car_vin[1]:
            return car_vin[1].strip()

        raise NoVinException("Vehicle doesnt have vin-code")

    def parse_detail_page(self, url: str) -> Tuple:
        car = self.get_page(
            url
        )

        return (
            self.get_username(car),
            self.get_images_count(car),
            self.get_phone_number(car)
        )

    def parse_single_car(self, car: Selector) -> Car:
        if car.xpath("//*[contains(@class, 'icon-sold-out')]"):
            raise SoldException("Vehicle already sold!")

        url = self.get_url(car)
        username, images_count, phone_number = self.parse_detail_page(url=url)
        return Car(
            url=url,
            title=self.get_title(car),
            price_usd=self.get_price_usd(car),
            odometer=self.get_odometer(car),
            username=username,
            phone_number=phone_number,
            image_url=self.get_image_url(car),
            images_count=images_count,
            car_number=self.get_car_number(car),
            car_vin=self.get_car_vin(car),
            datetime_found=datetime.now()
        )

    def get_single_page_cars(
        self,
        page_number: int
    ) -> None:
        page = self.get_page(
            urljoin(BASE_URL, f"?page={page_number}")
        )

        if not page.xpath("//*[contains(@class, 'ticket-item ')]"):
            raise EmptyPageException("This page is empty!")

        LOGGER.info(f"Parsing page {page_number}")

        cars = page.xpath("//*[contains(@class, 'ticket-item ')]").getall()
        cars_number = len(cars)
        for car in cars:
            try:
                self.results.append(
                    self.parse_single_car(Selector(text=car))
                )
            except (SoldException, NoVinException, NoUsernameException):
                cars_number -= 1
                continue
        LOGGER.info(
            f"Finished parsing page {page_number}. Cars number: {cars_number}."
        )


class AutoriaSpider:

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

    def get_data(self) -> None:
        current_page = 1

        LOGGER.info("Launched parser")

        try:
            while current_page <= self.pages:
                self.clean_threads()

                if len(self.threads) < self.max_threads:
                    parser = CarParser(self.results)
                    thread = threading.Thread(
                        target=parser.get_single_page_cars,
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
    parser = AutoriaSpider()
    parser.get_data()
