FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
COPY monitoring/events_writer.py /app/events_writer.py

RUN pip install --no-cache-dir -r /app/requirements.txt

COPY serving /app/serving
COPY feature_store /app/feature_store

ENV PYTHONPATH=/app
EXPOSE 8000

CMD ["uvicorn", "serving.app:app", "--host", "0.0.0.0", "--port", "8000"]