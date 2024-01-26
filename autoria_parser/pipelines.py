# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import os
from itemadapter import ItemAdapter
import psycopg2
from dotenv import load_dotenv

from .log import LOGGER


load_dotenv()


class AutoriaParserPipeline:
    
    def __init__(self) -> None:
        hostname = os.environ["POSTGRES_HOST"]
        username = os.environ["POSTGRES_USER"]
        password = os.environ["POSTGRES_PASSWORD"]
        database = os.environ["POSTGRES_DB"]
        
        self.connection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
        
        self.cur = self.connection.cursor()
        
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS cars(
            id serial PRIMARY KEY, 
            url TEXT,
            title VARCHAR(143),
            price_usd NUMERIC,
            odometer INT,
            username VARCHAR(143),
            phone_number TEXT,
            image_url TEXT,
            images_count INT,
            car_number VARCHAR(143),
            car_vin VARCHAR(143),
            datetime_found TIMESTAMP
        )
        """)
    
    def process_item(self, item, spider):
        self.cur.execute("""INSERT INTO cars (
            url,
            title,
            price_usd,
            odometer,
            username,
            phone_number,
            image_url,
            images_count,
            car_number,
            car_vin,
            datetime_found
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            item["url"],
            item["title"],
            item["price_usd"],
            item["odometer"],
            item["username"],
            item["phone_number"],
            item["image_url"],
            item["images_count"],
            item["car_number"],
            item["car_vin"],
            item["datetime_found"],
        ))
        
        self.connection.commit()
        return item

    def close_spider(self, spider):
        self.cur.close()
        self.connection.close()


class AutoriaParserNoDuplicatesPipeline(AutoriaParserPipeline):
    
    def process_item(self, item, spider):
        
        self.cur.execute(
            "SELECT * FROM cars WHERE car_vin = %s", (item["car_vin"],)
        )
        result = self.cur.fetchone()
        print(result)
        if result:
            LOGGER.warning("Item already in database. Vin: %s" % item["car_vin"])
        elif not item["car_vin"]:
            LOGGER.warning("Item without Vin skipped!")
        else:
            self.cur.execute("""INSERT INTO cars (
                url,
                title,
                price_usd,
                odometer,
                username,
                phone_number,
                image_url,
                images_count,
                car_number,
                car_vin,
                datetime_found
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                item["url"],
                item["title"],
                item["price_usd"],
                item["odometer"],
                item["username"],
                item["phone_number"],
                item["image_url"],
                item["images_count"],
                item["car_number"],
                item["car_vin"],
                item["datetime_found"],
            ))
            
            self.connection.commit()
        return item
