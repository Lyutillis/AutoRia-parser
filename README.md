# Auto Ria Parser
This project utilizes Scrapy to get data on used cars from `auto.ria.com` and saves it into PostgreSQL database.
- The code responsible for parsing is in `autoria_parser/spider/car_parser.py`
- All database actions are handled in `autoria_parser/pipelines.py`
- In `autoria_parser/celery.py` Celery settings for scheduled tasks are stored
- In `autoria_parser/log.py` Logger settings are stored

# .env
Overview:
- `POSTGRES_HOST=db` Hostname for PostgreSQL Database
- `POSTGRES_DB=cars` Name of the Database
- `POSTGRES_USER=postgres` Username
- `POSTGRES_PASSWORD=postgres` Password for the user
- `POSTGRES_PORT=5432` Database port, for PostgreSQL default is 5432
- `RABBIT_URL=amqp://guest:guest@rabbitmq3:5672/` RabbitMQ broker url
- `PAGES=10` Number of pages to be scraped
- `PARSE_HOUR=12` For daily parsing: hour
- `PARSE_MINUTE=0` For daily parsing: minutes
- `DUMP_HOUR=12` For daily database dump: hour
- `DUMP_MINUTE=0` For daily database dump: minutes

# Starting project locally
To run the project follow next steps:
1. Fork the repository

2. Clone it:
`git clone <here goes the HTTPS link you could copy on github repositiry page>`

3. Create a virtual environment:
`python3 -m venv venv`

4. Acivate venv:
- MAC `source venv/Scripts/activate`
- Windows `cd venv/Scripts/activate` -> `. activate`

5. Create .env file:
- You can copy .env.sample if you are going to use Docker.

6. Launch the project:
- With Docker:
`docker-compose up --build`
- Locally:
`scrapy crawl car_parser`
