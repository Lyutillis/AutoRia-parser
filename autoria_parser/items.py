from scrapy import Item, Field


class AutoriaParserItem(Item):
    url = Field()
    title = Field()
    price_usd = Field()
    odometer = Field()
    username = Field()
    phone_number = Field()
    image_url = Field()
    images_count = Field()
    car_number = Field()
    car_vin = Field()
    datetime_found = Field()
