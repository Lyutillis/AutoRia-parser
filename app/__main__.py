from .parser import Main
from .db import PostgresDB


if __name__ == "__main__":
    parser = Main()
    parser.run()
