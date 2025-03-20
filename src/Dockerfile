FROM python:3.12

WORKDIR /app

ENV DAGSTER_HOME=/app

ENV PYTHONPATH=/app

RUN apt-get update && apt-get install -y \
    gcc sudo vim nano telnet\
    && rm -rf /var/lib/apt/lists/*

COPY /etl/ /app/src/etl

COPY requirements.txt .

RUN pip install --default-timeout=1000 -r requirements.txt 

EXPOSE 3000

CMD ["dagster-webserver", "-f", "src/etl/definition.py", "-h", "0.0.0.0", "-p", "3000"]