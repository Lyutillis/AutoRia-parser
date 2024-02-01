from scrapy import cmdline
from celery import shared_task

from .pipelines import AutoriaParserPipeline


@shared_task
def run_spider() -> None:
    cmdline.execute("scrapy crawl car_parser".split())


@shared_task
def dump_db() -> None:
    db = AutoriaParserPipeline()
    db.create_database_dump()
