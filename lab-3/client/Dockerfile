FROM python:3.8-slim
WORKDIR /app
COPY ./client.py .
RUN pip install kafka-python
CMD ["python", "client.py"]
