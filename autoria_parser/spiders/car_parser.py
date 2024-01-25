import scrapy
import re
from datetime import datetime
from scrapy import Selector
from scrapy.http import Response
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import (
    TimeoutException,
    ElementNotInteractableException
)
import json


class CarParserSpider(scrapy.Spider):
    name = "car_parser"
    allowed_domains = ["auto.ria.com"]
    start_urls = ["https://auto.ria.com/uk/car/used/"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        options = webdriver.ChromeOptions()
        options.add_argument("headless")
        self.driver = webdriver.Chrome(options=options)
    
    def close(self, reason):
        self.driver.close()

    def parse(self, response: Response, **kwargs) -> dict:
        for car in response.css(".ticket-item"):
            if car.css(".icon-sold-out"):
                continue
            car_vin = car.css(".label-vin > span::text").get()
            if not car_vin:
                car_vin = car.css(".vin-code::text").get()
            url = response.urljoin(
                car.css(".content-bar > a.m-link-ticket::attr(href)").get()
            )
            car_data = {
                "url": url,
                "title": car.css(".ticket-title > a::attr(title)").get(),
                "price_usd": car.css(".price-ticket::attr(data-main-price)").get(),
                "odometer": car.css(".js-race::text").get(),
                "image_url": car.css("img.outline::attr(src)").get(),
                "car_number": car.css(".state-num::text").get(),
                "car_vin": car_vin,
                "datetime_found": datetime.now(),
            }
            yield scrapy.Request(
                url,
                callback=self._parse_detail,
                meta={"car_data": car_data}
            )

        next_page = response.css(".next > a.js-next").css("::attr(href)").get()
        num_page = response.css(".next > a.js-next").css("::attr(data-page)").get()

        if next_page is not None and int(num_page) < 3:
            next_page_url = response.urljoin(next_page)
            yield scrapy.Request(next_page_url, callback=self.parse)

    def _parse_detail(self, response: Response, **kwargs) -> dict:
        user_id = response.css("body::attr(data-auto-id)").get()
        user_hash = response.css('[class^="js-user-secure-"]::attr(data-hash)').get()
        expires = response.css('[class^="js-user-secure-"]::attr(data-expires)').get()
        phone_url = f"https://auto.ria.com/users/phones/{user_id}?hash={user_hash}&expires={expires}"
        car_data = {
            **response.meta["car_data"],
            "username": response.css(".seller_info_name::text").get(),
            "images_count": response.css("span.count > .mhide::text").get(),
        }
        yield scrapy.Request(
                phone_url,
                callback=self._parse_phone,
                meta={"car_data": car_data}
            ) 

    def _parse_phone(self, response: Response, **kwargs) -> dict:
        yield {
            **response.meta["car_data"],
            "phone_number": json.loads(response.text)["formattedPhoneNumber"],
        }
