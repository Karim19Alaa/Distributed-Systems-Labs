FROM python:3.8-slim
WORKDIR /app
RUN pip install kafka-python
COPY ./admin.py .
CMD ["python", "admin.py"]
