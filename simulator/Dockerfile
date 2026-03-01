FROM python:3.10-slim

WORKDIR /app

RUN pip install --no-cache-dir \
  confluent-kafka==2.3.0 \
  requests==2.31.0

COPY *.py /app/

ENV PYTHONUNBUFFERED=1

CMD ["python", "-u", "generate_claims.py"]