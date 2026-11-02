"""RetailPulse — Reference Loader (topics Kafka -> PostgreSQL).

Rôle :
    Consommer les topics de référence (`retail-products`, `retail-customers`)
    et insérer ou mettre à jour les données dans PostgreSQL.

Entrées :
    - Topic `retail-products`  (produits normalisés par le collecteur)
    - Topic `retail-customers` (clients normalisés par le collecteur)

Sorties :
    - Tables PostgreSQL `products` et `customers`.

Idempotence / rejouabilité :
    - Consommation depuis le début (`earliest`) avec un consumer group dédié ;
    - upsert `INSERT ... ON CONFLICT (clé primaire) DO UPDATE` sur les
      identifiants DummyJSON : le chargement est rejouable sans doublon ;
    - offsets commités APRÈS l'écriture en base (at-least-once + upsert
      idempotent => aucune donnée dupliquée).

Paramètres (variables d'environnement) :
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_PRODUCTS, KAFKA_TOPIC_CUSTOMERS,
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD,
    LOADER_IDLE_TIMEOUT_SECONDS (arrêt après N secondes sans message, défaut 20).
"""

import json
import logging
import os
import sys
import time
from contextlib import closing

import psycopg2
from confluent_kafka import Consumer, KafkaException

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_PRODUCTS = os.getenv("KAFKA_TOPIC_PRODUCTS", "retail-products")
TOPIC_CUSTOMERS = os.getenv("KAFKA_TOPIC_CUSTOMERS", "retail-customers")

POSTGRES = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "retailpulse"),
    "user": os.getenv("POSTGRES_USER", "retail"),
    "password": os.getenv("POSTGRES_PASSWORD", "retail"),
}
IDLE_TIMEOUT = int(os.getenv("LOADER_IDLE_TIMEOUT_SECONDS", "20"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("reference-loader")

# ---------------------------------------------------------------------------
# Requêtes d'upsert (idempotentes, rejouables)
# ---------------------------------------------------------------------------
UPSERT_PRODUCT = """
INSERT INTO products
    (product_id, title, description, category, brand, price,
     discount_percentage, rating, stock, thumbnail, updated_at)
VALUES
    (%(product_id)s, %(title)s, %(description)s, %(category)s, %(brand)s,
     %(price)s, %(discount_percentage)s, %(rating)s, %(stock)s,
     %(thumbnail)s, NOW())
ON CONFLICT (product_id) DO UPDATE SET
    title = EXCLUDED.title,
    description = EXCLUDED.description,
    category = EXCLUDED.category,
    brand = EXCLUDED.brand,
    price = EXCLUDED.price,
    discount_percentage = EXCLUDED.discount_percentage,
    rating = EXCLUDED.rating,
    stock = EXCLUDED.stock,
    thumbnail = EXCLUDED.thumbnail,
    updated_at = EXCLUDED.updated_at;
"""

UPSERT_CUSTOMER = """
INSERT INTO customers
    (customer_id, first_name, last_name, email, age, city, country, updated_at)
VALUES
    (%(customer_id)s, %(first_name)s, %(last_name)s, %(email)s,
     %(age)s, %(city)s, %(country)s, NOW())
ON CONFLICT (customer_id) DO UPDATE SET
    first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name,
    email = EXCLUDED.email,
    age = EXCLUDED.age,
    city = EXCLUDED.city,
    country = EXCLUDED.country,
    updated_at = EXCLUDED.updated_at;
"""


def upsert(cursor, topic: str, payload: dict) -> None:
    """Applique l'upsert correspondant au topic source."""
    if topic == TOPIC_PRODUCTS:
        cursor.execute(UPSERT_PRODUCT, payload)
    elif topic == TOPIC_CUSTOMERS:
        cursor.execute(UPSERT_CUSTOMER, payload)
    else:
        raise ValueError(f"Topic inattendu : {topic}")


def main() -> int:
    log.info("Reference loader — Kafka=%s, PG=%s:%s/%s",
             KAFKA_BOOTSTRAP, POSTGRES["host"], POSTGRES["port"], POSTGRES["dbname"])

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "reference-loader",
        "enable.auto.commit": False,          # commit manuel après écriture
        "auto.offset.reset": "earliest",       # relecture complète du référentiel
    })

    try:
        # Connexion PostgreSQL (transaction autocommit par lot)
        conn = psycopg2.connect(**POSTGRES)
        conn.autocommit = False
        consumer.subscribe([TOPIC_PRODUCTS, TOPIC_CUSTOMERS])

        counts = {TOPIC_PRODUCTS: 0, TOPIC_CUSTOMERS: 0}
        errors = 0
        last_message_time = time.time()
        assigned = False

        log.info("Consommation des topics de référence (arrêt après %ds d'inactivité)...", IDLE_TIMEOUT)
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # Aucun message : si les partitions sont affectées et que le
                # flux est silencieux depuis IDLE_TIMEOUT, le chargement est fini.
                if assigned and (time.time() - last_message_time) > IDLE_TIMEOUT:
                    log.info("Aucun nouveau message depuis %ds : fin du chargement.", IDLE_TIMEOUT)
                    break
                continue

            if msg.error():
                log.error("Erreur consommateur : %s", msg.error())
                if msg.error().fatal():
                    raise KafkaException(msg.error())
                continue

            if not assigned:
                assigned = True  # première affectation de partition
            last_message_time = time.time()

            try:
                payload = json.loads(msg.value().decode("utf-8"))
                with closing(conn.cursor()) as cursor:
                    upsert(cursor, msg.topic(), payload)
                conn.commit()               # transaction validée par message
                consumer.commit(asynchronous=False)  # offset commité après écriture
                counts[msg.topic()] += 1
            except (ValueError, KeyError, psycopg2.Error) as exc:
                conn.rollback()
                errors += 1
                log.error("Message rejeté (topic=%s, offset=%s) : %s",
                          msg.topic(), msg.offset(), exc)

        log.info("Chargement terminé : %s produits, %s clients, %d erreurs.",
                 counts[TOPIC_PRODUCTS], counts[TOPIC_CUSTOMERS], errors)
        return 1 if errors else 0

    except (KafkaException, psycopg2.OperationalError) as exc:
        log.error("Échec du chargement : %s", exc)
        return 1
    finally:
        # Fermeture propre des ressources Kafka.
        consumer.close()
        try:
            conn.close()
        except Exception:  # noqa: BLE001 — la connexion peut ne pas exister
            pass


if __name__ == "__main__":
    sys.exit(main())
