from datetime import datetime
import json
from typing import Any


class DateTimeEncoder(json.JSONEncoder):
    def default(self, o: Any):
        if isinstance(o, datetime):
            return o.isoformat()

        return super().default(o)
