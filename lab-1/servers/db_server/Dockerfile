FROM python:3.9-slim-buster

WORKDIR /app

COPY ./db_server/db_server.py .
COPY ./commons /app/commons/

CMD ["python", "db_server.py"]