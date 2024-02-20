from sys import stdout
from loguru import logger


class Logger:
    @classmethod
    def create_logger(cls) -> "Logger":
        """Create custom logger."""
        logger.remove()
        logger.add(
            stdout,
            colorize=True,
            level="INFO",
            catch=True,
            format="<light-cyan>{time:MM-DD-YYYY HH:mm:ss}</light-cyan> | "
            + "<light-green>{level}</light-green>: "
            + "<light-white>{message}</light-white>",
        )
        return logger


LOGGER = Logger.create_logger()
