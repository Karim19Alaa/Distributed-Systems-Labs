FROM python:3.8-slim
WORKDIR /app
RUN pip install kafka-python
RUN pip install requests
COPY ./client.py .
CMD ["python", "client.py"]
