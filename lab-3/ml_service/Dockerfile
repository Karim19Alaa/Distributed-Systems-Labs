FROM python:3.8-slim
WORKDIR /app
COPY ./ml_service.py .
RUN pip install kafka-python
CMD ["python", "ml_service.py"]
