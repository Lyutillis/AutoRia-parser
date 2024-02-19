import os
import sys
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

from dto import Car
from log import LOGGER


load_dotenv(verbose=True, override=True)


class PostgresDB:

    def __init__(self) -> None:
        self.hostname = os.environ["POSTGRES_HOST"]
        self.username = os.environ["POSTGRES_USER"]
        self.password = os.environ["POSTGRES_PASSWORD"]
        self.database = os.environ["POSTGRES_DB"]
        self.port = os.environ["POSTGRES_PORT"]

        self.connection = psycopg2.connect(
            host=self.hostname,
            user=self.username,
            password=self.password,
            dbname=self.database,
        )
        
        self.connection.autocommit = True

        self.cur = self.connection.cursor()

        self.cur.execute(
            """
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
            """
        )

    def __enter__(self) -> None:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.cur.close()
        self.connection.close()

    def create_database_dump(self) -> None:
        timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        file_path = os.path.join("dumps", f"dump_{timestamp}")
        command = (
            f"pg_dump --no-owner --dbname=postgresql://{self.username}"
            f":{self.password}@{self.hostname}:{self.port}/"
            f"{self.database} > {file_path}"
        )
        code = os.system(command)
        if code:
            LOGGER.error(
                f"Error dumping database: code - {code}, command - {command}"
            )
            sys.exit(10)

    def process_item(self, item: Car) -> dict:
        self.cur.execute(
            """INSERT INTO cars (
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
        """,
            (
                item.url,
                item.title,
                item.price_usd,
                item.odometer,
                item.username,
                item.phone_number,
                item.image_url,
                item.images_count,
                item.car_number,
                item.car_vin,
                item.datetime_found,
            ),
        )

        self.connection.commit()
        return item


class PostgresNoDuplicatesDB(PostgresDB):

    def process_item(self, item: Car) -> dict:

        self.cur.execute(
            "SELECT * FROM cars WHERE car_vin = %s", (item.car_vin,)
        )
        result = self.cur.fetchone()
        if result:
            LOGGER.warning(
                "Item already in database. Vin: %s" % item.car_vin
            )
        elif not item.car_vin:
            LOGGER.warning("Item without Vin skipped!")
        else:
            self.cur.execute(
                """INSERT INTO cars (
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
            """,
                (
                    item.url,
                    item.title,
                    item.price_usd,
                    item.odometer,
                    item.username,
                    item.phone_number,
                    item.image_url,
                    item.images_count,
                    item.car_number,
                    item.car_vin,
                    item.datetime_found,
                ),
            )

            self.connection.commit()
        return item
