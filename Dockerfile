FROM --platform=linux/amd64 python:3.10.9-slim-buster
LABEL maintainer="chemuranov@gmail.com"

RUN apt-get update && apt-get -y install cron vim tzdata

WORKDIR /app

COPY crontab /etc/cron.d/crontab
RUN chmod 0644 /etc/cron.d/crontab

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apt-get update

RUN pip install --upgrade pip
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . .

RUN /usr/bin/crontab /etc/cron.d/crontab

RUN touch /var/log/cron.log

CMD ["cron", "-f"]
