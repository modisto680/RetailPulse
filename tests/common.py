"""RetailPulse — utilitaires communs aux tests d'intégration.

Ces helpers publient des événements CONFORMES au contrat du producteur
(`producer/producer.py`) et interrogent PostgreSQL pour vérifier les
résultats produits par le pipeline Spark.

Exécution depuis l'hôte (docker-compose up) : les valeurs par défaut
`localhost:9092` / `localhost:5432` conviennent.
"""

import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone

import psycopg2
from confluent_kafka import Producer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_ORDERS = os.getenv("KAFKA_TOPIC_ORDERS", "retail-orders")
TOPIC_EVENTS = os.getenv("KAFKA_TOPIC_ORDER_EVENTS", "retail-order-events")

PG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "retailpulse"),
    "user": os.getenv("POSTGRES_USER", "retail"),
    "password": os.getenv("POSTGRES_PASSWORD", "retail"),
}


def iso(dt: datetime) -> str:
    """Horodatage ISO 8601 UTC avec millisecondes (format du contrat)."""
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def now_iso() -> str:
    return iso(datetime.now(timezone.utc))


def past_iso(minutes: float) -> str:
    return iso(datetime.now(timezone.utc) - timedelta(minutes=minutes))


def new_order_id() -> str:
    return f"test-{uuid.uuid4().hex[:12]}"


def publish(producer: Producer, topic: str, key: str, payload: dict) -> None:
    producer.produce(topic, key=key.encode("utf-8"),
                     value=json.dumps(payload, ensure_ascii=False).encode("utf-8"))
    producer.poll(0)


def make_producer() -> Producer:
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})


def pick_product(cur) -> tuple[int, str, float]:
    """Prend un produit du référentiel : (product_id, category, price)."""
    cur.execute("SELECT product_id, category, price FROM products ORDER BY RANDOM() LIMIT 1")
    row = cur.fetchone()
    if not row:
        raise RuntimeError("Table products vide : exécuter le collecteur + reference loader.")
    return row[0], row[1], float(row[2])


def pick_customer(cur) -> int:
    cur.execute("SELECT customer_id FROM customers ORDER BY RANDOM() LIMIT 1")
    row = cur.fetchone()
    if not row:
        raise RuntimeError("Table customers vide : exécuter le collecteur + reference loader.")
    return row[0]


def make_order_event(order_id: str, customer_id: int, product_id: int,
                     category: str, price: float, quantity: int = 1,
                     timestamp: str | None = None) -> dict:
    """ORDER_CREATED conforme au contrat du producteur."""
    amount = round(quantity * price, 2)
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "ORDER_CREATED",
        "event_timestamp": timestamp or now_iso(),
        "order_id": order_id,
        "customer_id": customer_id,
        "items": [{
            "product_id": product_id, "title": "(test)", "category": category,
            "quantity": quantity, "unit_price": round(price, 2),
        }],
        "amount": amount,
        "currency": "USD",
    }


def make_payment_event(event_type: str, order_event: dict,
                        timestamp: str | None = None,
                        amount: float | None = None) -> dict:
    """Événement de paiement/annulation conforme au contrat.

    `amount` par défaut : celui de la commande (payé si COMPLETED).
    `event_id` est paramétrable via `timestamp`/event_id pour les tests de
    doublons : passer le MÊME dictionnaire deux fois pour rejouer.
    """
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_timestamp": timestamp or now_iso(),
        "order_id": order_event["order_id"],
        "customer_id": order_event["customer_id"],
        "items": order_event["items"],
        "amount": round(amount if amount is not None else order_event["amount"], 2),
        "currency": "USD",
        "payment_method": "credit_card",
        "reason": None,
    }


def scalar(sql: str, params: tuple = ()) -> float | None:
    """Exécute une requête scalaire ; None si le résultat est NULL."""
    with psycopg2.connect(**PG) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return row[0] if row else None


def wait_for(description: str, predicate, timeout: float = 90.0, interval: float = 3.0) -> bool:
    """Sonde jusqu'à succès ou timeout (trigger Spark = 10 s par défaut)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            print(f"  [OK] {description}")
            return True
        time.sleep(interval)
    print(f"  [ÉCHEC] {description} (timeout {timeout}s)")
    return False
