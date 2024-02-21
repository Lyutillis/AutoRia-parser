from .parser import AutoriaSpider
from .db import PostgresDB


if __name__ == "__main__":
    parser = AutoriaSpider()
    parser.get_data()
