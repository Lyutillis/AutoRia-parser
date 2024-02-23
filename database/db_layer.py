import os
import sys
import psycopg2
from datetime import datetime
from typing import List
from dataclasses import astuple
import asyncio
import asyncpg

from utils.dto import Car
from utils.log import get_logger
from utils.metaclasses import Singleton
import envs


db_logger = get_logger("Database")


class PostgresDB(Singleton):
    def __init__(self) -> None:
        self.hostname = envs.POSTGRES_HOST
        self.username = envs.POSTGRES_USER
        self.password = envs.POSTGRES_PASSWORD
        self.database = envs.POSTGRES_DB
        self.port = envs.POSTGRES_PORT

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
                db_logger.warning("Item already in database. Vin: %s" % vin)
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
            db_logger.error(
                f"Error dumping database: code - {code}, command - {command}"
            )
            sys.exit(10)


class AsyncPostgresDB(Singleton):
    async def __init__(self) -> None:
        self.hostname = envs.POSTGRES_HOST
        self.username = envs.POSTGRES_USER
        self.password = envs.POSTGRES_PASSWORD
        self.database = envs.POSTGRES_DB
        self.port = envs.POSTGRES_PORT
        self.pool = await self.connect()

        async with self.pool.acquire() as connection:
            async with connection.transaction():
                await self.connection.execute(
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

    async def connect(self):
        return await asyncpg.create_pool(
            host=self.hostname,
            user=self.username,
            password=self.password,
            database=self.database,
            port=self.port
        )

    def __enter__(self) -> None:
        return self

    async def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.connection.close()

    async def process_items(self, items: List[Car]) -> None:
        if not items:
            return

        async with self.pool.acquire() as connection:
            async with connection.transaction():
                vins = [item.car_vin for item in items]
                result = await connection.fetchval(
                    "SELECT array_agg(car_vin) "
                    "FROM cars WHERE car_vin = ANY($1)",
                    vins
                )

            if result:
                for vin in result:
                    db_logger.warning(
                        "Item already in database. Vin: %s" % vin
                    )
                items = [item for item in items if item.car_vin not in result]

            if items:
                values = [item.as_tuple() for item in items]
                await connection.executemany(
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
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    """,
                    values
                )

    async def create_database_dump(self) -> None:
        timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        file_path = os.path.join("dumps", f"dump_{timestamp}")
        command = (
            f"pg_dump --no-owner --dbname=postgresql://{self.username}"
            f":{self.password}@{self.hostname}:{self.port}/"
            f"{self.database} > {file_path}"
        )
        code = await asyncio.create_subprocess_shell(command)
        if code:
            db_logger.error(
                f"Error dumping database: code - {code}, command - {command}"
            )
            sys.exit(10)
