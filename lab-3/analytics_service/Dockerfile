# analytics_service/Dockerfile
FROM python:3.8-slim
WORKDIR /app
COPY ./analytics_service.py .
RUN pip install kafka-python
CMD ["python", "analytics_service.py"]
