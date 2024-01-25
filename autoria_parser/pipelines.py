# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import psycopg2


class AutoriaParserPipeline:
    
    def __init__(self) -> None:
        hostname = ""
        username = ""
        password = ""
        database = ""
        
        self.connection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
        
        self.cur = self.connection.cursor()
        
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS cars(
            id serial PRIMARY KEY, 
            url text,
            title VARCHAR(143),
            price_usd MONEY,
            odometer INT,
            username VARCHAR(143),
            phone_number text,
            image_url text,
            images_count INT,
            car_number VARCHAR(143),
            car_vin VARCHAR(143) UNIQUE,
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
        """, **item)
