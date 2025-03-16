# analytics_service/Dockerfile
FROM python:3.8-slim
WORKDIR /app
RUN pip install kafka-python
COPY ./analytics_service.py .
CMD ["python", "analytics_service.py"]
