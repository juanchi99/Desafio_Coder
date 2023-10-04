FROM python:3.11.3-alpine
FROM apache/airflow:2.7.1

WORKDIR /app

COPY . .

RUN pip install -r requirements.txt &&\
    rm requirements.txt
