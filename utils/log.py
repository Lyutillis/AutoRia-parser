from loguru import logger


def get_logger(name: str, rotation="5 MB") -> logger:
    logger.add(
        f"./logs/{name}.log",
        format="{time} {level} {name}:{function}:{line} {message}"
    )
    return logger


if __name__ == "__main__":
    logger_ = get_logger("logger")
    result = logger_.info("hello")
    print(result)
