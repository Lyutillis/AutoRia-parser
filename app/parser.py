import requests
from urllib.parse import urljoin
from datetime import datetime
from typing import List, Tuple
from parsel import Selector
import json
import threading

from db import PostgresNoDuplicatesDB, Car
from exceptions import EmptyPageException


BASE_URL = "https://auto.ria.com/uk/car/used/"
PHONE_URL = "https://auto.ria.com/users/phones/"


def parse_detail_page(url: str) -> Tuple:
    print(url)
    page = requests.get(url).content
    html = Selector(body=page)

    user_id = html.xpath("//body/@data-auto-id").get()
    user_hash = html.xpath(
        "//*[starts-with(@class, 'js-user-secure-')]/@data-hash"
    ).get()
    expires = html.xpath(
        "//*[starts-with(@class, 'js-user-secure-')]/@data-expires"
    ).get()

    phone_url = urljoin(
        PHONE_URL, f"{user_id}?hash={user_hash}&expires={expires}"
    )
    phone_number = "+38" + json.loads(
        requests.get(phone_url).text
    )["formattedPhoneNumber"]

    username = html.xpath(
        "//*[contains(@class, 'seller_info_name')]//a/text()"
    ).get()
    if not username:
        username = html.xpath(
            "//*[contains(@class, 'seller_info_name')]/text()"
        ).get()

    images_count = html.xpath(
        (
            "//span[contains(@class, 'count')]"
            "//*[contains(@class, 'mhide')]/text()"
        )
    ).get().split()[1]
    print(username.strip(), images_count, phone_number)
    return username.strip(), images_count, phone_number


def parse_single_car(car: Selector) -> Car:
    if car.xpath("//*[contains(@class, 'icon-sold-out')]"):
        return None

    url = car.xpath(
        (
            "//*[contains(@class, 'content-bar')]"
            "//a[contains(@class, 'm-link-ticket')]/@href"
        )
    ).get()
    car_number = car.xpath("//*[contains(@class, 'state-num')]/text()").get()
    if car_number:
        car_number = car_number.strip()
    title = car.xpath("//*[contains(@class, 'ticket-title')]//a/@href").get()
    price_usd = car.xpath(
        "//*[contains(@class, 'price-ticket')]/@data-main-price"
    ).get()
    odometer = car.xpath("//*[contains(@class, 'js-race')]/text()").get()
    image_url = car.xpath("//img[contains(@class, 'outline')]/@src").get()
    datetime_found = datetime.now()
    car_vin = (
        car.xpath("//*[contains(@class, 'label-vin')]//span/text()").get(),
        car.xpath("//*[contains(@class, 'vin-code')]/text()").get(),
    )

    if car_vin[0]:
        car_vin = car_vin[0].strip()
    elif car_vin[1]:
        car_vin = car_vin[1].strip()
    else:
        car_vin = None
    username, images_count, phone_number = parse_detail_page(url=url)
    return Car(
        url,
        title,
        float(price_usd),
        float(odometer.split()[0]),
        username,
        phone_number,
        image_url,
        images_count,
        car_number,
        car_vin,
        datetime_found
    )


def get_single_page_cars(
    page_number: int,
    db: PostgresNoDuplicatesDB
) -> None:
    response = requests.get(
        urljoin(BASE_URL, f"?page={page_number}")
    ).content
    page = Selector(body=response)

    if not page.xpath("//*[contains(@class, 'ticket-item ')]"):
        raise EmptyPageException("This page is empty!")

    cars = page.xpath("//*[contains(@class, 'ticket-item ')]").getall()

    for car in cars:
        result = parse_single_car(Selector(text=car))
        if result:
            db.process_item(result)


def get_cars() -> None:
    with PostgresNoDuplicatesDB() as db:
        pages = 100
        current_page = 1
        # threads = []

        try:
            while current_page <= pages:
                # thread = threading.Thread(
                #     target=get_single_page_cars, args=(current_page, db,)
                # )
                # thread.start()
                # threads.append(thread)
                get_single_page_cars(current_page, db)
                # print(parse_detail_page("https://auto.ria.com/uk/auto_mercedes_benz_s_class_36078502.html"))
                current_page += 1
        except EmptyPageException:
            pass

        # for thread in threads:
        #     thread.join()


if __name__ == "__main__":
    get_cars()
