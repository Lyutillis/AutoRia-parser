import redis

import envs


class Cache:
    def __init__(
        self,
        number_db: int,
        host: str = envs.REDIS_HOST,
        port: int = envs.REDIS_PORT,
        password: str = envs.REDIS_PASSWORD
    ) -> None:
        self.red = redis.Redis(
            host=host,
            port=port,
            db=number_db,
            password=password,
            decode_responses=True
        )


class AsyncCache:
    def __init__(
        self,
        number_db: int,
        host: str = envs.REDIS_HOST,
        port: int = envs.REDIS_PORT,
        password: str = envs.REDIS_PASSWORD
    ) -> None:
        self.red = redis.asyncio.Redis(
            host=host,
            port=port,
            db=number_db,
            password=password,
            decode_responses=True
        )
