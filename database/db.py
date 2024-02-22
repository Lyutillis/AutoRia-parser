import os
import sys
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
from typing import List
from dataclasses import astuple

from utils.dto import Car
from utils.log import LOGGER
from utils.metaclasses import Singleton


load_dotenv(verbose=True, override=True)


class PostgresDB(Singleton):
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

    def process_items(self, items: List[Car]) -> None:
        if not items:
            return

        self.cur.execute(
            "SELECT car_vin FROM cars WHERE car_vin IN %(vins)s",
            {
                "vins": tuple([item.car_vin for item in items])
            },
        )
        result = [vin[0] for vin in self.cur.fetchall()]
        if result:
            for vin in result:
                LOGGER.warning("Item already in database. Vin: %s" % vin)
            items = [item for item in items if item.car_vin not in result]

        if items:
            args = ",".join(
                self.cur.mogrify(
                    "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    astuple(item)
                ).decode("utf-8")
                for item in items
            )
            if args:
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
                    ) VALUES """
                    + (args)
                )

                self.connection.commit()

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
