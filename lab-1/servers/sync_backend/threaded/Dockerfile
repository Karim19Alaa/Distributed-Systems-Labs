FROM python:3.9-slim-buster

WORKDIR /app

COPY ./sync_backend/threaded/threaded_server.py .
COPY ./commons/ /app/commons/

CMD ["python", "threaded_server.py"]