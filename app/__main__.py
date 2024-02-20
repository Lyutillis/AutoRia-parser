from .parser import CarParser
from .db import PostgresDB


if __name__ == "__main__":
    parser = CarParser()
    parser.parse_cars()
