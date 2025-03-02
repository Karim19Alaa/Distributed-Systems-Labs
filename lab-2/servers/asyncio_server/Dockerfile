FROM python:3.9-slim-buster

WORKDIR /app

COPY ./asyncio_server/asyncio_server.py .
COPY ./commons/ /app/commons/

CMD ["python", "asyncio_server.py"]