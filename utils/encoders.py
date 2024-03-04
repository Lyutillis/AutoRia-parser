from datetime import datetime
import json
from bson import ObjectId
from typing import Any


class DateTimeEncoder(json.JSONEncoder):
    def default(self, o: Any):
        if isinstance(o, datetime):
            return datetime.timestamp(o)

        return super().default(o)


class ObjectIdEncoder(json.JSONEncoder):
    def default(self, o: Any):
        if isinstance(o, ObjectId):
            return str(o)

        return super().default(o)
