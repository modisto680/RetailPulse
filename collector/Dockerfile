# RetailPulse — API Collector
# Image Python légère ; le collecteur est un Job ponctuel (pas de serveur).
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY collector.py .

# L'utilisateur non-root est une bonne pratique conteneur.
RUN useradd --create-home appuser
USER appuser

ENTRYPOINT ["python", "collector.py"]
