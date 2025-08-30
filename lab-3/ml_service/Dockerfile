FROM python:3.8-slim
WORKDIR /app
RUN pip install kafka-python
RUN pip install requests
COPY ./ml_service.py .
CMD ["python", "ml_service.py"]
