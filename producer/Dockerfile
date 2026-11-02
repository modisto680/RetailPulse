# RetailPulse — Producteur de simulation (service continu)
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

RUN useradd --create-home appuser
USER appuser

ENTRYPOINT ["python", "producer.py"]
