FROM python:3.9-slim-buster

WORKDIR /app

COPY ./sync_backend/sequential/sequential_server.py .
COPY ./commons/ /app/commons/

CMD ["python", "sequential_server.py"]