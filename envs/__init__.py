from environs import Env
import os

try:
    os.mkdir("logs")
except Exception:
    pass

env = Env()

try:
    env.read_env(".env", override=True)

    POSTGRES_HOST = env.str("POSTGRES_HOST")
    POSTGRES_USER = env.str("POSTGRES_USER")
    POSTGRES_PASSWORD = env.str("POSTGRES_PASSWORD")
    POSTGRES_DB = env.str("POSTGRES_DB")
    POSTGRES_PORT = env.int("POSTGRES_PORT")
    REDIS_HOST = env.str("REDIS_HOST")
    REDIS_PORT = env.int("REDIS_PORT")
    REDIS_PASSWORD = env.str("REDIS_PASSWORD")
    MONGO_HOST = env.str("MONGO_HOST")
    MONGO_PORT = env.int("MONGO_PORT")
    MONGO_DB = env.str("MONGO_DB")
    MONGO_USER = env.str("MONGO_USER")
    MONGO_PASSWORD = env.str("MONGO_PASSWORD")

    PAGES = env.int("PAGES")

except Exception as e:
    print(e)
    exit()


if __name__ == "__main__":
    print(PAGES)
