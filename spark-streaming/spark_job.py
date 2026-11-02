"""RetailPulse — Spark Structured Streaming.

Rôle :
    Consommer les topics `retail-orders` et `retail-order-events`, parser et
    contrôler les champs obligatoires, filtrer les paiements, déduplicquer par
    event_id, enrichir avec le catalogue produits, agréger par catégorie sur
    des fenêtres de 5 minutes et persister dans PostgreSQL (upsert).

Entrées :
    - Topics Kafka `retail-orders` et `retail-order-events` (contrat JSON) ;
    - Table PostgreSQL `products` (enrichissement, join statique broadcasté).

Sorties (toutes idempotentes, upsert ON CONFLICT) :
    - Table `orders`           : créée par ORDER_CREATED, statut mis à jour
                                  par les événements de paiement (composant
                                  identifié : CE job) ;
    - Table `sales_aggregates`  : agrégats par catégorie et fenêtre de 5 min
                                  (seuls les PAYMENT_COMPLETED comptent) ;
    - Topic `retail-events-dlq` : messages invalides (dead letter queue).

Événements tardifs / watermark :
    Watermark de 10 minutes sur `event_timestamp`. Les événements plus tardifs
    que le watermark sont abandonnés ; ceux qui arrivent dans le délai mettent
    à jour leur fenêtre (mode update + upsert cumulatif en base).

Déduplication :
    `dropDuplicates(["event_id"])` APRÈS le watermark : un event_id rejoué
    dans le délai du watermark n'est comptabilisé qu'une seule fois. La clé
    UNIQUE(category, window_start, window_end) en base constitue une défense
    en profondeur pour l'idempotence des agrégats.

Reprise après incident :
    Chaque requête de streaming possède son répertoire de checkpoint
    persistant (PVC en Kubernetes, volume Docker en local). Au redémarrage,
    Spark reprend depuis les offsets et l'état sauvegardés.

Paramètres (variables d'environnement) : voir .env.example.
"""

import logging
import os
import sys
from contextlib import closing

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, DoubleType, LongType, StringType,
                               StructField, StructType)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_ORDERS = os.getenv("KAFKA_TOPIC_ORDERS", "retail-orders")
TOPIC_EVENTS = os.getenv("KAFKA_TOPIC_ORDER_EVENTS", "retail-order-events")
TOPIC_DLQ = os.getenv("KAFKA_TOPIC_DLQ", "retail-events-dlq")

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "retailpulse")
PG_USER = os.getenv("POSTGRES_USER", "retail")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "retail")
JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
PG_PROPS = {"user": PG_USER, "password": PG_PASSWORD, "driver": "org.postgresql.Driver"}

def pg_connect():
    return psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                            user=PG_USER, password=PG_PASSWORD)

WATERMARK = os.getenv("SPARK_WATERMARK_DELAY", "10 minutes")
WINDOW = os.getenv("SPARK_WINDOW_DURATION", "5 minutes")
TRIGGER = os.getenv("SPARK_TRIGGER_INTERVAL", "10 seconds")
CHECKPOINT_ROOT = os.getenv("SPARK_CHECKPOINT_DIR", "/opt/spark/checkpoint")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("spark-job")

# Format d'horodatage du contrat d'événement : ISO 8601 UTC (ex. 2026-11-02T10:15:00.123Z)
TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"

# ---------------------------------------------------------------------------
# Schéma du contrat d'événement (champs obligatoires du cahier des charges §7)
# ---------------------------------------------------------------------------
ITEM_SCHEMA = StructType([
    StructField("product_id", LongType(), nullable=False),
    StructField("title", StringType(), nullable=True),
    StructField("category", StringType(), nullable=True),
    StructField("quantity", LongType(), nullable=False),
    StructField("unit_price", DoubleType(), nullable=False),
])

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=False),        # déduplication
    StructField("event_type", StringType(), nullable=False),       # type métier
    StructField("event_timestamp", StringType(), nullable=False), # ISO 8601
    StructField("order_id", StringType(), nullable=False),        # cycle commande
    StructField("customer_id", LongType(), nullable=False),        # référentiel
    StructField("items", ArrayType(ITEM_SCHEMA), nullable=True),   # lignes
    StructField("amount", DoubleType(), nullable=False),           # montant payé
    StructField("currency", StringType(), nullable=True),
    StructField("payment_method", StringType(), nullable=True),
    StructField("reason", StringType(), nullable=True),
])

# ---------------------------------------------------------------------------
# Écritures PostgreSQL (upsert idempotents)
# ---------------------------------------------------------------------------
UPSERT_ORDER = """
INSERT INTO orders (order_id, customer_id, created_at, status,
                    total_amount, currency, item_count, updated_at)
VALUES (%(order_id)s, %(customer_id)s, %(event_time)s, 'PENDING',
        %(amount)s, %(currency)s, %(item_count)s, NOW())
ON CONFLICT (order_id) DO UPDATE SET
    total_amount = EXCLUDED.total_amount,
    currency     = EXCLUDED.currency,
    item_count   = EXCLUDED.item_count,
    updated_at   = NOW();
"""

UPDATE_STATUS = """
UPDATE orders
SET status = CASE %(event_type)s
                WHEN 'PAYMENT_COMPLETED' THEN 'PAID'
                WHEN 'PAYMENT_FAILED'   THEN 'PAYMENT_FAILED'
                WHEN 'ORDER_CANCELLED'   THEN 'CANCELLED'
             END,
    paid_at   = CASE WHEN %(event_type)s = 'PAYMENT_COMPLETED' THEN %(event_time)s END,
    updated_at = NOW()
WHERE order_id = %(order_id)s;
"""

UPSERT_AGGREGATE = """
INSERT INTO sales_aggregates
    (category, window_start, window_end, order_count, total_quantity,
     total_revenue, updated_at)
VALUES
    (%(category)s, %(window_start)s, %(window_end)s, %(order_count)s,
     %(total_quantity)s, %(total_revenue)s, NOW())
ON CONFLICT (category, window_start, window_end) DO UPDATE SET
    order_count     = EXCLUDED.order_count,
    total_quantity  = EXCLUDED.total_quantity,
    total_revenue   = EXCLUDED.total_revenue,
    updated_at      = NOW();
"""


# ---------------------------------------------------------------------------
# foreachBatch : table orders (création + statut de paiement)
# ---------------------------------------------------------------------------
def process_orders(batch_df, batch_id: int):
    """Persiste les événements dans `orders` : ORDER_CREATED => upsert,
    événements de paiement/annulation => mise à jour du statut."""
    if batch_df.isEmpty():
        return
    created = 0
    updated = 0
    with closing(pg_connect()) as conn:
        with closing(conn.cursor()) as cur:
            for row in batch_df.collect():
                ev = row["event"]
                params = {
                    "order_id": ev["order_id"],
                    "customer_id": ev["customer_id"],
                    "event_time": row["event_ts"],
                    "amount": ev["amount"],
                    "currency": ev["currency"] or "USD",
                    "item_count": len(ev["items"] or []),
                    "event_type": ev["event_type"],
                }
                try:
                    if ev["event_type"] == "ORDER_CREATED":
                        cur.execute(UPSERT_ORDER, params)
                        created += 1
                    else:  # PAYMENT_COMPLETED / PAYMENT_FAILED / ORDER_CANCELLED
                        cur.execute(UPDATE_STATUS, params)
                        if cur.rowcount == 0:
                            # Commande inconnue (ORDER_CREATED manquant ou en DLQ)
                            log.warning("[batch %d] %s ignoré : commande %s inconnue",
                                        batch_id, ev["event_type"], ev["order_id"])
                        updated += 1
                except psycopg2.Error as exc:
                    # Ex : customer_id absent du référentiel => événement tracé,
                    # le lot continue (les erreurs sont visibles dans les logs).
                    log.error("[batch %d] ordre %s non persisté : %s",
                              batch_id, ev.get("order_id"), exc)
                    conn.rollback()
        conn.commit()
    log.info("[batch %d] orders : %d créées / mises à jour, %d statuts",
             batch_id, created, updated)


# ---------------------------------------------------------------------------
# foreachBatch : table sales_aggregates (fenêtres 5 min / catégorie)
# ---------------------------------------------------------------------------
def process_aggregates(batch_df, batch_id: int):
    """Upsert des agrégats : en mode `update`, chaque ligne émise porte la
    valeur CUMULÉE de sa fenêtre => l'upsert écrase avec EXCLUDED (idempotent)."""
    if batch_df.isEmpty():
        return
    rows = [
        {
            "category": r["category"],
            "window_start": r["window"]["start"],
            "window_end": r["window"]["end"],
            "order_count": r["order_count"],
            "total_quantity": r["total_quantity"],
            "total_revenue": float(r["total_revenue"]),
        }
        for r in batch_df.collect()
    ]
    if not rows:
        return
    with closing(pg_connect()) as conn:
        with closing(conn.cursor()) as cur:
            cur.executemany(UPSERT_AGGREGATE, rows)
        conn.commit()
    log.info("[batch %d] sales_aggregates : %d fenêtre(s)/catégorie upsertée(s)",
             batch_id, len(rows))


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
def main() -> int:
    spark = (
        SparkSession.builder
        .appName("retailpulse-streaming")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "6")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark démarré — Kafka=%s, watermark=%s, window=%s, trigger=%s",
             KAFKA_BOOTSTRAP, WATERMARK, WINDOW, TRIGGER)

    # Référentiel produits (statique, ~200 lignes => broadcast)
    products = (
        spark.read.jdbc(JDBC_URL, "(SELECT product_id, category FROM products) AS p", PG_PROPS)
        .cache()
    )
    log.info("Référentiel produits chargé pour l'enrichissement : %d lignes",
             products.count())

    # --- 1. Source Kafka : les deux topics d'événements ---------------------
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", f"{TOPIC_ORDERS},{TOPIC_EVENTS}")
        .option("startingOffsets", "latest")
        .load()
    )

    # --- 2. Parsing JSON + colonnes utiles ----------------------------------
    parsed_json = raw.select(
        F.col("key"),
        F.col("value"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event"),
    )

    # Conversion de l'horodatage ISO 8601 du contrat (second select : la
    # colonne 'event' devient référencable pour to_timestamp).
    parsed = parsed_json.withColumn(
        "event_ts", F.to_timestamp(F.col("event.event_timestamp"), TS_FORMAT)
    )

    # --- 3. Validation : champs obligatoires du contrat ---------------------
    # event_id, event_type, order_id, customer_id, timestamp, amount et items.
    is_valid = (
        F.col("event.event_id").isNotNull()
        & F.col("event.event_type").isNotNull()
        & F.col("event.order_id").isNotNull()
        & F.col("event.customer_id").isNotNull()
        & F.col("event.amount").isNotNull()
        & F.col("event.event_type").isin(
            "ORDER_CREATED", "PAYMENT_COMPLETED", "PAYMENT_FAILED", "ORDER_CANCELLED")
        & F.col("event_ts").isNotNull()
    )

    valid = parsed.where(is_valid)
    invalid = parsed.where(~is_valid)

    # --- 4. Watermark + déduplication par event_id --------------------------
    # event_id est hissé au premier niveau : dropDuplicates exige une colonne
    # de haut niveau ; le watermark borne l'état de déduplication.
    deduped = (
        valid
        .withColumn("event_id", F.col("event.event_id"))
        .withWatermark("event_ts", WATERMARK)
        .dropDuplicates(["event_id"])
    )

    # --- 5. Requête A : persistance des commandes et statuts ----------------
    orders_query = (
        deduped.writeStream
        .outputMode("append")
        .foreachBatch(process_orders)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/orders")
        .trigger(processingTime=TRIGGER)
        .queryName("orders-to-postgresql")
        .start()
    )

    # --- 6. Requête B : CA par catégorie, fenêtres de 5 minutes --------------
    #    Seuls les PAYMENT_COMPLETED sont retenus (règle de chiffre d'affaires).
    paid = deduped.where(F.col("event.event_type") == "PAYMENT_COMPLETED")

    aggregates = (
        paid
        .select(
            F.col("event.order_id").alias("order_id"),
            F.col("event_ts"),
            F.explode(F.col("event.items")).alias("item"),
        )
        # Enrichissement par le catalogue : la catégorie du référentiel fait
        # foi ; celle de l'événement sert de repli, sinon 'unknown'.
        .join(
            F.broadcast(products),
            F.col("item.product_id") == F.col("product_id"),
            "left_outer",
        )
        .select(
            F.coalesce(F.col("category"), F.col("item.category"),
                       F.lit("unknown")).alias("category"),
            F.col("event_ts"),
            F.col("order_id"),
            F.col("item.quantity").alias("quantity"),
            F.col("item.unit_price").alias("unit_price"),
        )
        .groupBy(F.window(F.col("event_ts"), WINDOW), F.col("category"))
        .agg(
            F.countDistinct(F.col("order_id")).alias("order_count"),
            F.sum(F.col("quantity")).alias("total_quantity"),
            F.sum(F.col("quantity") * F.col("unit_price")).alias("total_revenue"),
        )
    )

    aggregates_query = (
        aggregates.writeStream
        .outputMode("update")
        .foreachBatch(process_aggregates)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/aggregates")
        .trigger(processingTime=TRIGGER)
        .queryName("aggregates-to-postgresql")
        .start()
    )

    # --- 7. Requête C : messages invalides -> dead letter queue -------------
    dlq_query = (
        invalid.select(F.col("key"), F.col("value"))
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", TOPIC_DLQ)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/dlq")
        .trigger(processingTime=TRIGGER)
        .queryName("invalid-events-to-dlq")
        .start()
    )

    log.info("3 requêtes de streaming actives (orders, aggregates, dlq).")

    # --- 8. Supervision : échec => exit 1 => Kubernetes redémarre le Pod -----
    try:
        spark.streams.awaitAnyTermination()
    except Exception as exc:  # noqa: BLE001 — toute erreur de requête est fatale
        log.error("Requête de streaming interrompue : %s", exc)
        return 1

    failed = [q.name for q in spark.streams.active if not q.isActive]
    log.warning("Fin du job. Requêtes terminées : %s", failed or "aucune")
    return 0


if __name__ == "__main__":
    sys.exit(main())
