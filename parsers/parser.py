import requests
from urllib.parse import urljoin
from datetime import datetime
from parsel import Selector
import json

from utils.dto import Car
from utils.exceptions import (
    NoVinException,
    SoldException,
    NoUsernameException
)


PHONE_URL = "https://auto.ria.com/users/phones/"


class AutoriaParser:
    def __init__(self, html: str, url: str) -> None:
        self.html = Selector(text=html)
        self.url = url

    @classmethod
    def validate(cls, html: str):
        page = Selector(text=html)

        if (
            page.xpath("//*[contains(@class, 'app-head')]")
            and page.xpath("//*[contains(@class, 'footer-line-wrap')]")
        ):
            return True
        return False

    @classmethod
    def check_list_page(self, html: str):
        page = Selector(text=html)
        if not page.xpath("//*[contains(@class, 'ticket-item ')]"):
            return False
        return True

    @classmethod
    def get_urls(cls, html: str) -> str:
        page = Selector(text=html)
        return page.xpath(
            (
                "//*[contains(@class, 'content-bar')]"
                "//a[contains(@class, 'm-link-ticket')]/@href"
            )
        ).getall()


class AutoriaParserV1(AutoriaParser):

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

        raise NoUsernameException("Unable to parse the username!")

    def get_phone_number(self) -> str:
        user_id = self.url.replace(".html", "").split("_")[-1]
        user_hash = self.html.xpath(
            "//*[starts-with(@class, 'js-user-secure-')]//@data-hash"
        ).get()
        expires = self.html.xpath(
            "//*[starts-with(@class, 'js-user-secure-')]//@data-expires"
        ).get()
        phone_url = urljoin(
            PHONE_URL, f"{user_id}?hash={user_hash}&expires={expires}"
        )
        response = json.loads(
            requests.get(phone_url, stream=False).text
        )["formattedPhoneNumber"]
        return "+38" + response

    def get_image_url(self) -> str:
        return self.html.xpath(
            (
                "//div[contains(@class, 'carousel-inner')]"
                "//div//source/@srcset"
            )
        ).get()

    def get_images_count(self) -> int:
        count = self.html.xpath(
            (
                "//span[contains(@class, 'count')]"
                "//*[contains(@class, 'mhide')]/text()"
            )
        ).get()
        if count:
            return int(count.split()[1])
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

        if self.html.xpath("//*[contains(@class, 'sold-out')]"):
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


class AutoriaParserV2(AutoriaParser):

    def get_title(self) -> str:
        return self.html.xpath(
            "//h1[contains(@class, 'titleXl')]/text()"
        ).get()

    def get_price_usd(self) -> float:
        price_usd = self.html.xpath(
            "//div[@id='sidePrice']//strong[contains(@class, 'titleL')]/text()"
        ).get()
        for char in ("$", "грн", "€"):
            price_usd = price_usd.replace(char, "")
        return float(price_usd.strip().replace(" ", ""))

    def get_odometer(self) -> float:
        odometer = self.html.xpath(
            "//div[@id='basicInfoTableMainInfoLeft0']//span/text()"
        ).get().split()[0]
        return float(odometer)

    def get_username(self) -> str:
        username = self.html.xpath(
            "//div[@id='sellerInfoUserName')]//span/text()"
        ).get(),
        return username.strip()

    def get_phone_number(self) -> str:
        phone_number = self.html.xpath(
            "(//div[@id='autoPhonePopUpResponse')]"
            "//button[@class='s1 conversion']//span/text()"
        ).get()

        return "+38" + phone_number

    def get_image_url(self) -> str:
        return self.html.xpath(
            (
                "//div[contains(@class, 'swiper-slide-active')]"
                "//source/@srcset"
            )
        ).get()

    def get_images_count(self) -> int:
        count = self.html.xpath(
            (
                "//span[@class='common-badge alpha medium')]"
                "//span/text()"
            )
        ).getall()
        if count:
            return int(count[1])
        return 0

    def get_car_number(self) -> str:
        car_number = self.html.xpath(
            "//div[contains(@class, 'car-number')]//span/text()"
        ).get()
        if car_number:
            car_number = car_number.strip()
        return car_number

    def get_car_vin(self) -> str:
        car_vin = self.html.xpath(
            "//*[@id='badgesVinGrid')]"
            "//span[contains(@class, 'common-text body')//text()"
        ).get()

        if car_vin:
            return car_vin.strip()
        return car_vin

    def parse_detail_page(self) -> Car:

        if self.html.xpath("//*[contains(@class, 'selled-auto')]"):
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
