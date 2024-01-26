import scrapy
import re
from datetime import datetime
from scrapy import Selector
from scrapy.http import Response
import json


class CarParserSpider(scrapy.Spider):
    name = "car_parser"
    allowed_domains = ["auto.ria.com"]
    start_urls = ["https://auto.ria.com/uk/car/used/?page=1"]

    def parse(self, response: Response, **kwargs) -> dict:
        for car in response.css(".ticket-item"):
            if car.css(".icon-sold-out"):
                continue
            car_vin = car.css(".label-vin > span::text").get()
            if not car_vin:
                car_vin = car.css(".vin-code::text").get()
            if car_vin:
                car_vin = car_vin.strip()
            url = response.urljoin(
                car.css(".content-bar > a.m-link-ticket::attr(href)").get()
            )
            car_number = car.css(".state-num::text").get()
            if car_number:
                car_number = car_number.strip()
            car_data = {
                "url": url,
                "title": car.css(".ticket-title > a::attr(title)").get(),
                "price_usd": float(car.css(".price-ticket::attr(data-main-price)").get()),
                "odometer": float(car.css(".js-race::text").get().split()[0]),
                "image_url": car.css("img.outline::attr(src)").get(),
                "car_number": car_number,
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

        if next_page is not None and int(num_page) < 10:
            next_page_url = response.urljoin(next_page)
            yield scrapy.Request(next_page_url, callback=self.parse)

    def _parse_detail(self, response: Response, **kwargs) -> dict:
        user_id = response.css("body::attr(data-auto-id)").get()
        user_hash = response.css('[class^="js-user-secure-"]::attr(data-hash)').get()
        expires = response.css('[class^="js-user-secure-"]::attr(data-expires)').get()
        phone_url = f"https://auto.ria.com/users/phones/{user_id}?hash={user_hash}&expires={expires}"
        username = response.css(".seller_info_name > a::text").get()
        if not username:
            username = response.css(".seller_info_name::text").get()
        car_data = {
            **response.meta["car_data"],
            "username": username.strip(),
            "images_count": int(response.css("span.count > .mhide::text").get().split()[1]),
        }
        yield scrapy.Request(
                phone_url,
                callback=self._parse_phone,
                meta={"car_data": car_data}
            ) 

    def _parse_phone(self, response: Response, **kwargs) -> dict:
        yield {
            **response.meta["car_data"],
            "phone_number": "+38" + json.loads(response.text)["formattedPhoneNumber"],
        }
