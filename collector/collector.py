"""RetailPulse — API Collector (DummyJSON -> Kafka).

Rôle :
    Collecter les données de référence (produits et clients) depuis l'API
    publique DummyJSON, les valider, les normaliser et les publier dans les
    topics Kafka de référence.

Entrées :
    - https://dummyjson.com/products?limit=0  (catalogue produits)
    - https://dummyjson.com/users?limit=0    (référentiel clients)

Sorties :
    - Topic `retail-products`  : un message JSON par produit, clé = product_id
    - Topic `retail-customers` : un message JSON par client,  clé = customer_id

Paramètres (variables d'environnement) :
    KAFKA_BOOTSTRAP_SERVERS    ex. kafka:29092
    KAFKA_TOPIC_PRODUCTS       défaut retail-products
    KAFKA_TOPIC_CUSTOMERS      défaut retail-customers
    DUMMYJSON_BASE_URL         défaut https://dummyjson.com
    DUMMYJSON_TIMEOUT_SECONDS  défaut 30

Gestion des erreurs :
    - 3 tentatives avec backoff exponentiel par endpoint ;
    - timeout HTTP configurable ;
    - les éléments invalides (champs obligatoires manquants) sont comptés et
      exclus, la collecte continue ;
    - si un endpoint échoue définitivement, le Job échoue (exit != 0) pour être
      relancé par Kubernetes.

Traçabilité :
    Volumes collectés / publiés / rejetés, erreurs et durée imprimés en fin
    d'exécution (résumé JSON) et datés.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_PRODUCTS = os.getenv("KAFKA_TOPIC_PRODUCTS", "retail-products")
TOPIC_CUSTOMERS = os.getenv("KAFKA_TOPIC_CUSTOMERS", "retail-customers")
BASE_URL = os.getenv("DUMMYJSON_BASE_URL", "https://dummyjson.com").rstrip("/")
TIMEOUT = int(os.getenv("DUMMYJSON_TIMEOUT_SECONDS", "30"))
MAX_RETRIES = 3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("collector")


class CollectorError(Exception):
    """Erreur de collecte non récupérable après tentatives."""


# ---------------------------------------------------------------------------
# Collecte HTTP (pagination DummyJSON gérée par limit=0 : tout en une réponse)
# ---------------------------------------------------------------------------
def fetch_endpoint(path: str) -> list[dict]:
    """Interroge un endpoint DummyJSON avec tentatives et backoff.

    DummyJSON expose la pagination via total/skip/limit ; `limit=0` renvoie
    tous les éléments en une seule réponse. On vérifie néanmoins `total` vs
    la taille du tableau reçu pour détecter une réponse incomplète.
    """
    url = f"{BASE_URL}{path}"
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info("GET %s (tentative %d/%d)", url, attempt, MAX_RETRIES)
            response = requests.get(url, timeout=TIMEOUT)
            response.raise_for_status()
            payload = response.json()
            items = payload.get("products", []) if "products" in path else payload.get("users", [])
            total = payload.get("total")
            if total is not None and len(items) != total:
                raise CollectorError(
                    f"Réponse incomplète pour {url} : {len(items)}/{total} éléments"
                )
            log.info("Réponse OK : %s éléments (total annoncé=%s)", len(items), total)
            return items
        except (requests.RequestException, CollectorError, ValueError) as exc:
            last_error = exc
            log.warning("Échec : %s", exc)
            time.sleep(2**attempt)  # backoff exponentiel : 2s, 4s, 8s
    raise CollectorError(f"Échec définitif de la collecte pour {url} : {last_error}")


# ---------------------------------------------------------------------------
# Validation et normalisation
# ---------------------------------------------------------------------------
REQUIRED_PRODUCT_FIELDS = ("id", "title", "category", "price", "stock")
REQUIRED_CUSTOMER_FIELDS = ("id", "firstName", "lastName", "email")


def normalize_product(raw: dict) -> dict | None:
    """Normalise un produit DummyJSON vers le modèle relationnel cible.

    Conserve l'identifiant source (id DummyJSON). Retourne None si un champ
    obligatoire est manquant ou invalide.
    """
    try:
        if any(raw.get(f) is None for f in REQUIRED_PRODUCT_FIELDS):
            return None
        if not isinstance(raw["price"], (int, float)) or float(raw["price"]) < 0:
            return None
        return {
            "product_id": int(raw["id"]),
            "title": str(raw.get("title", "")).strip(),
            "description": str(raw.get("description", ""))[:1000],
            "category": str(raw.get("category", "unknown")).strip(),
            "brand": str(raw.get("brand") or "unknown").strip(),
            "price": round(float(raw["price"]), 2),
            "discount_percentage": round(float(raw.get("discountPercentage") or 0.0), 2),
            "rating": round(float(raw.get("rating") or 0.0), 2),
            "stock": int(raw["stock"]),
            "thumbnail": str(raw.get("thumbnail") or ""),
        }
    except (TypeError, ValueError, KeyError) as exc:
        log.warning("Produit invalide (id=%s) : %s", raw.get("id"), exc)
        return None


def normalize_customer(raw: dict) -> dict | None:
    """Normalise un client DummyJSON vers le modèle relationnel cible."""
    try:
        if any(raw.get(f) is None for f in REQUIRED_CUSTOMER_FIELDS):
            return None
        address = raw.get("address") or {}
        return {
            "customer_id": int(raw["id"]),
            "first_name": str(raw["firstName"]).strip(),
            "last_name": str(raw["lastName"]).strip(),
            "email": str(raw["email"]).strip().lower(),
            "age": int(raw["age"]) if raw.get("age") is not None else None,
            "city": str(address.get("city") or "unknown").strip(),
            "country": str(address.get("country") or "unknown").strip(),
        }
    except (TypeError, ValueError, KeyError) as exc:
        log.warning("Client invalide (id=%s) : %s", raw.get("id"), exc)
        return None


# ---------------------------------------------------------------------------
# Publication Kafka
# ---------------------------------------------------------------------------
def delivery_report(err, msg):
    """Callback de livraison : trace les échecs de publication."""
    if err is not None:
        log.error("Livraison Kafka échouée : %s", err)


def publish(producer: Producer, topic: str, key: str, payload: dict) -> None:
    """Publie un message JSON avec une clé de partitionnement."""
    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        on_delivery=delivery_report,
    )


# ---------------------------------------------------------------------------
# Programme principal
# ---------------------------------------------------------------------------
def main() -> int:
    started = datetime.now(timezone.utc)
    log.info("Collecte RetailPulse — %s", started.isoformat())
    log.info("Kafka=%s | topics: %s, %s", KAFKA_BOOTSTRAP, TOPIC_PRODUCTS, TOPIC_CUSTOMERS)

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    stats = {"products": 0, "customers": 0, "rejected_products": 0, "rejected_customers": 0}

    try:
        # --- Produits -------------------------------------------------------
        products = fetch_endpoint("/products?limit=0")
        for raw in products:
            product = normalize_product(raw)
            if product is None:
                stats["rejected_products"] += 1
                continue
            publish(producer, TOPIC_PRODUCTS, str(product["product_id"]), product)
            stats["products"] += 1

        # --- Clients --------------------------------------------------------
        users = fetch_endpoint("/users?limit=0")
        for raw in users:
            customer = normalize_customer(raw)
            if customer is None:
                stats["rejected_customers"] += 1
                continue
            publish(producer, TOPIC_CUSTOMERS, str(customer["customer_id"]), customer)
            stats["customers"] += 1

        # Garantir l'envoi avant de fermer proprement les ressources Kafka.
        remaining = producer.flush(30)
        if remaining > 0:
            log.error("%d message(s) non acquitté(s) à la fermeture", remaining)

    except CollectorError as exc:
        log.error("Collecte interrompue : %s", exc)
        producer.flush(10)
        return 1

    duration = (datetime.now(timezone.utc) - started).total_seconds()
    summary = {
        "collected_at": started.isoformat(),
        "duration_seconds": round(duration, 2),
        **stats,
    }
    log.info("RÉSUMÉ COLLECTE : %s", json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
