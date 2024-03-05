FROM --platform=linux/amd64 python:3.10.9-slim-buster
LABEL maintainer="chemuranov@gmail.com"

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN  apt-get update \
    && apt-get install -y wget \
    && rm -rf /var/lib/apt/lists/*

RUN apt-get update -qq && apt-get install -y \
    postgresql-client

RUN wget https://fastdl.mongodb.org/tools/db/mongodb-database-tools-debian92-x86_64-100.9.4.deb && \
    apt install ./mongodb-database-tools-*.deb && \
    rm -f mongodb-database-tools-*.deb

RUN pip install --upgrade pip
COPY orchestrator_requirements.txt orchestrator_requirements.txt
RUN pip install -r orchestrator_requirements.txt

COPY ../ .

CMD ["python3", "-m", "m_orchestrator"]
