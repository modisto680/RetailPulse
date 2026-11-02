# RetailPulse — Reference Loader (Job ponctuel)
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY reference_loader.py .

RUN useradd --create-home appuser
USER appuser

ENTRYPOINT ["python", "reference_loader.py"]
