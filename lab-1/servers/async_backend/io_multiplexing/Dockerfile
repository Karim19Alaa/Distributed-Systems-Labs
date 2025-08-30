FROM python:3.9-slim-buster

WORKDIR /app

COPY ./async_backend/io_multiplexing/io_multiplexing_server.py .
COPY ./commons/ /app/commons/

CMD ["python", "io_multiplexing_server.py"]