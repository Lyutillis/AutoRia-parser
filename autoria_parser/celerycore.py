from __future__ import absolute_import, unicode_literals
from celery import Celery
from celery.schedules import crontab
import os
from dotenv import load_dotenv


load_dotenv(verbose=True, override=True)


app = Celery(
    "celerycore",
    broker=os.environ["RABBIT_URL"],
)
app.conf.enable_utc = False

app.conf.update(
    timezone="Europe/Kiev",
    task_serializer="json",
    accept_content=["application/json"],
    result_serializer="json",
    broker_connection_retry_on_startup=True,
    include=["autoria_parser.tasks"],
)


app.conf.beat_schedule = {
    "parse-cars": {
        "task": "autoria_parser.tasks.run_spider",
        "schedule": crontab(
            minute=os.environ["PARSE_MINUTE"],
            hour=os.environ["PARSE_HOUR"]
        ),
    },
    "dump-cars": {
        "task": "autoria_parser.tasks.dump_db",
        "schedule": crontab(
            minute=os.environ["DUMP_MINUTE"],
            hour=os.environ["DUMP_HOUR"]
        ),
    },
}
