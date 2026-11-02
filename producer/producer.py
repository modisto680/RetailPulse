#!/usr/bin/env python
"""RetailPulse — Producteur de simulation commerciale (continue).

Rôle :
    Exploiter les référentiels (chargés depuis PostgreSQL), sélectionner des
    clients et des produits, puis générer une activité commerciale continue :
    commandes puis résultats de paiement.

Entrées :
    - Tables PostgreSQL `products` et `customers` (référentiels DummyJSON).

Sorties :
    - Topic `retail-orders` : événements ORDER_CREATED (clé = order_id) ;
    - Topic `retail-order-events` : PAYMENT_COMPLETED, PAYMENT_FAILED ou
      ORDER_CANCELLED (clé = order_id).

Contrat d'événement (champs obligatoires) :
    event_id          UUID v4 unique — base de la déduplication ;
    event_type        type métier ;
    event_timestamp   ISO 8601 UTC (ex. 2026-11-02T10:15:00.123Z) ;
    order_id          relie les événements d'un même cycle de commande ;
    customer_id       client issu du référentiel ;
    items             [{product_id, quantity, unit_price, category}] ;
    amount / currency montant TOTAL PAYÉ (somme quantité x prix unitaire,
                      prix catalogue brut), devise USD.

Règle métier :
    Le producteur émet AU PLUS UN événement terminal par commande. Un paiement
    complété n'est jamais annulé ensuite : aucune compensation du chiffre
    d'affaires n'est nécessaire (règle définie dans docs/architecture.md).

Paramètres (variables d'environnement) :
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_ORDERS, KAFKA_TOPIC_ORDER_EVENTS,
    POSTGRES_*, PRODUCER_MIN_INTERVAL_MS (déf. 500),
    PRODUCER_MAX_INTERVAL_MS (déf. 2000),
    PRODUCER_PAYMENT_COMPLETED_RATE (déf. 0.85),
    PRODUCER_PAYMENT_FAILED_RATE (déf. 0.10),
    PRODUCER_DUPLICATE_RATE (déf. 0.0 — réémet le même event_id pour la démo),
    PRODUCER_MAX_ITEMS_PER_ORDER (déf. 4).

Arrêt propre :
    SIGTERM / SIGINT => fin de la boucle courante, flush Kafka (30 s max),
    fermeture des connexions PostgreSQL et Kafka.
"""

import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

import psycopg2
from confluent_kafka import Producer

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_ORDERS = os.getenv("KAFKA_TOPIC_ORDERS", "retail-orders")
TOPIC_EVENTS = os.getenv("KAFKA_TOPIC_ORDER_EVENTS", "retail-order-events")

POSTGRES = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "retailpulse"),
    "user": os.getenv("POSTGRES_USER", "retail"),
    "password": os.getenv("POSTGRES_PASSWORD", "retail"),
}

MIN_INTERVAL = float(os.getenv("PRODUCER_MIN_INTERVAL_MS", "500")) / 1000.0
MAX_INTERVAL = float(os.getenv("PRODUCER_MAX_INTERVAL_MS", "2000")) / 1000.0
RATE_COMPLETED = float(os.getenv("PRODUCER_PAYMENT_COMPLETED_RATE", "0.85"))
RATE_FAILED = float(os.getenv("PRODUCER_PAYMENT_FAILED_RATE", "0.10"))
RATE_DUPLICATE = float(os.getenv("PRODUCER_DUPLICATE_RATE", "0.0"))
MAX_ITEMS = int(os.getenv("PRODUCER_MAX_ITEMS_PER_ORDER", "4"))

CURRENCY = "USD"
PAYMENT_METHODS = ["credit_card", "paypal", "bank_transfer"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("producer")

running = True


def handle_stop(signum, _frame):
    """Arrêt propre : SIGTERM (Kubernetes) et SIGINT (Ctrl+C)."""
    global running
    log.info("Signal %s reçu — arrêt propre en cours...", signum)
    running = False


signal.signal(signal.SIGTERM, handle_stop)
signal.signal(signal.SIGINT, handle_stop)


# ---------------------------------------------------------------------------
# Chargement des référentiels depuis PostgreSQL
# ---------------------------------------------------------------------------
def load_references(conn):
    """Charge les produits et clients depuis PostgreSQL."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT product_id, title, category, price, stock "
            "FROM products ORDER BY product_id"
        )
        products = [
            {"product_id": r[0], "title": r[1], "category": r[2],
             "price": float(r[3]), "stock": r[4]}
            for r in cur.fetchall()
        ]
        cur.execute("SELECT customer_id FROM customers ORDER BY customer_id")
        customers = [r[0] for r in cur.fetchall()]
    if not products or not customers:
        raise RuntimeError(
            "Référentiels vides : exécuter d'abord le collecteur puis le "
            "reference loader (tables products / customers)."
        )
    log.info("Référentiels chargés : %d produits, %d clients.", len(products), len(customers))
    return products, customers


# ---------------------------------------------------------------------------
# Génération des événements
# ---------------------------------------------------------------------------
def utc_now_iso() -> str:
    """Horodatage ISO 8601 UTC avec millisecondes."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def build_order_event(customer_id: int, products: list[dict]) -> dict:
    """Génère une commande : 1 à MAX_ITEMS produits, quantités et prix."""
    picked = random.sample(products, k=random.randint(1, min(MAX_ITEMS, len(products))))
    items = [
        {
            "product_id": p["product_id"],
            "title": p["title"],
            "category": p["category"],
            "quantity": random.randint(1, 3),
            "unit_price": round(p["price"], 2),
        }
        for p in picked
    ]
    amount = round(sum(i["quantity"] * i["unit_price"] for i in items), 2)
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "ORDER_CREATED",
        "event_timestamp": utc_now_iso(),
        "order_id": f"ord-{uuid.uuid4().hex[:12]}",
        "customer_id": customer_id,
        "items": items,
        "amount": amount,           # montant total de la commande (brut)
        "currency": CURRENCY,
    }


def build_payment_event(order_event: dict) -> dict:
    """Génère l'événement terminal du cycle de commande.

    Tirage : PAYMENT_COMPLETED (RATE_COMPLETED), PAYMENT_FAILED (RATE_FAILED),
    sinon ORDER_CANCELLED. Le paiement transporte les mêmes items et le même
    montant que la commande : le montant PAYÉ est `amount`.
    """
    draw = random.random()
    if draw < RATE_COMPLETED:
        event_type = "PAYMENT_COMPLETED"
    elif draw < RATE_COMPLETED + RATE_FAILED:
        event_type = "PAYMENT_FAILED"
    else:
        event_type = "ORDER_CANCELLED"

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_timestamp": utc_now_iso(),
        "order_id": order_event["order_id"],
        "customer_id": order_event["customer_id"],
        "items": order_event["items"],
        "amount": order_event["amount"],  # montant payé si COMPLETED
        "currency": order_event["currency"],
        "payment_method": random.choice(PAYMENT_METHODS),
        "reason": None if event_type == "PAYMENT_COMPLETED" else random.choice(
            ["insufficient_funds", "card_declined", "customer_request"]
        ),
    }


# ---------------------------------------------------------------------------
# Publication Kafka
# ---------------------------------------------------------------------------
def delivery_report(err, msg):
    if err is not None:
        log.error("Livraison Kafka échouée : %s", err)


def publish(producer: Producer, topic: str, key: str, payload: dict) -> None:
    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        on_delivery=delivery_report,
    )


# ---------------------------------------------------------------------------
# Boucle de simulation
# ---------------------------------------------------------------------------
def main() -> int:
    log.info(
        "Producteur RetailPulse — intervalle %.2fs-%.2fs | completed=%.2f "
        "failed=%.2f cancelled=%.2f | doublons=%.2f",
        MIN_INTERVAL, MAX_INTERVAL, RATE_COMPLETED, RATE_FAILED,
        1.0 - RATE_COMPLETED - RATE_FAILED, RATE_DUPLICATE,
    )

    conn = psycopg2.connect(**POSTGRES)
    products, customers = load_references(conn)
    conn.close()  # le producteur ne lit plus PostgreSQL ensuite

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    stats = {
        "orders": 0, "completed": 0, "failed": 0, "cancelled": 0, "duplicates": 0,
    }
    started = time.time()

    try:
        while running:
            order = build_order_event(random.choice(customers), products)
            publish(producer, TOPIC_ORDERS, order["order_id"], order)
            stats["orders"] += 1

            payment = build_payment_event(order)
            publish(producer, TOPIC_EVENTS, order["order_id"], payment)
            stats[payment["event_type"].split("_", 1)[1].lower()] += 1

            # Injection optionnelle d'un doublon (même event_id) pour la
            # démonstration de la déduplication Spark.
            if payment["event_type"] == "PAYMENT_COMPLETED" and random.random() < RATE_DUPLICATE:
                publish(producer, TOPIC_EVENTS, order["order_id"], payment)
                stats["duplicates"] += 1

            producer.poll(0)  # traite les callbacks de livraison

            # Résumé périodique toutes les 50 commandes.
            if stats["orders"] % 50 == 0:
                elapsed = time.time() - started
                log.info(
                    "[stat] %.0fs | %d commandes | %.2f cmd/s | %s",
                    elapsed, stats["orders"], stats["orders"] / max(elapsed, 1e-9),
                    json.dumps({k: v for k, v in stats.items() if k != "orders"}),
                )

            time.sleep(random.uniform(MIN_INTERVAL, MAX_INTERVAL))

    finally:
        # Arrêt propre : flush des messages en attente puis fermeture Kafka.
        remaining = producer.flush(30)
        if remaining:
            log.error("%d message(s) non acquitté(s) à l'arrêt.", remaining)
        log.info("Producteur arrêté. Résumé : %s", json.dumps(stats))
    return 0


if __name__ == "__main__":
    sys.exit(main())
