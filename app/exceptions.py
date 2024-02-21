from app.log import LOGGER


class EmptyPageException(Exception):
    pass


class NoVinException(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)

        LOGGER.warning("Item without Vin skipped!")


class SoldException(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)

        LOGGER.warning("Sold item skipped!")


class NoUsernameException(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)

        LOGGER.warning("Item without username skipped!")


class NotLoadedPageException(Exception):
    pass
