import requests
from urllib.parse import urljoin
from datetime import datetime
from typing import List, Tuple
from parsel import Selector
import json
import threading
import time

from .db import PostgresDB, Car
from .exceptions import (
    EmptyPageException,
    NoVinException,
    SoldException,
    NoUsernameException
)


BASE_URL = "https://auto.ria.com/uk/car/used/"
PHONE_URL = "https://auto.ria.com/users/phones/"


class CarParser:

    def get_url(self, car: Selector) -> str:
        return car.xpath(
            (
                "//*[contains(@class, 'content-bar')]"
                "//a[contains(@class, 'm-link-ticket')]/@href"
            )
        ).get()

    def get_title(self, car: Selector) -> str:
        return car.xpath(
            "//*[contains(@class, 'ticket-title')]//a/@href"
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
        return int(
            car.xpath(
                (
                    "//span[contains(@class, 'count')]"
                    "//*[contains(@class, 'mhide')]/text()"
                )
            ).get().split()[1]
        )

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
        page = requests.get(url, stream=False).text
        car = Selector(text=page)

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
            title=self.get_url(car),
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
        response = requests.get(
            urljoin(BASE_URL, f"?page={page_number}"),
            stream=False
        ).text
        page = Selector(text=response)

        if not page.xpath("//*[contains(@class, 'ticket-item ')]"):
            raise EmptyPageException("This page is empty!")

        cars = page.xpath("//*[contains(@class, 'ticket-item ')]").getall()

        for car in cars:
            try:
                result = self.parse_single_car(Selector(text=car))
                with PostgresDB() as db:
                    db.process_item(result)
            except (SoldException, NoVinException, NoUsernameException):
                continue

    def parse_cars(self) -> None:
        pages = 100
        current_page = 1
        threads = []

        try:
            while current_page <= pages:
                thread = threading.Thread(
                    target=self.get_single_page_cars, args=(current_page,)
                )
                thread.start()
                threads.append(thread)
                current_page += 1
        except EmptyPageException:
            pass

        for thread in threads:
            thread.join()
