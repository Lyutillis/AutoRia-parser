from utils.log import get_logger


exc_logger = get_logger("Exceptions")


class EmptyPageException(Exception):
    pass


class NoVinException(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)

        exc_logger.warning("Item without Vin skipped!")


class SoldException(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)

        exc_logger.warning("Sold item skipped!")


class NoUsernameException(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)

        exc_logger.warning("Item without username skipped!")


class NotLoadedPageException(Exception):
    pass
