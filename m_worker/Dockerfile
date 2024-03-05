FROM ubuntu:20.04
LABEL maintainer="chemuranov@gmail.com"

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apt-get update

RUN apt -y install python3.9 \
    && apt-get -y install python3-pip

RUN pip install --upgrade pip

RUN pip install playwright==1.41.2 && \
    playwright install --with-deps

COPY worker_requirements.txt worker_requirements.txt
RUN pip install -r worker_requirements.txt

COPY ../ .

CMD ["python3", "-m", "m_worker"]
