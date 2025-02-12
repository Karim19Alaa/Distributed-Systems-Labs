FROM python:3.9-slim-buster

WORKDIR /app

COPY ./sync_backend/threadpool/threadpool_server.py .
COPY ./commons/ /app/commons/

CMD ["python", "threadpool_server.py"]