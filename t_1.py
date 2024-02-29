from utils.cache import Cache
import json
from utils.dto import Task

if __name__ == "__main__":
    # cache_0 = Cache(0)
    # cache_0.red.lpush(
    #     "test_queue",
    #     json.dumps(
    #         {
    #             "key": {
    #                 "another key": 1
    #             }
    #         }
    #     )
    # )
    # result = cache_0.red.rpop("test_queue")
    # print(result)
    data = {
        "id": 1,
        "page_number": 2,
        "in_work": True,
        "completed": False,
    }
    result = Task(**data)
    print(result)
