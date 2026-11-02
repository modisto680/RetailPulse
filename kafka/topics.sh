#!/usr/bin/env bash
# RetailPulse — création des topics Kafka (KRaft, sans ZooKeeper).
# Usage local (docker-compose) : ./kafka/topics.sh
# Usage manuel : BOOTSTRAP=localhost:9092 ./kafka/topics.sh
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
PARTITIONS="${KAFKA_NUM_PARTITIONS:-3}"
RF="${KAFKA_REPLICATION_FACTOR:-1}"

# Adapté au conteneur : script par défaut d'Apache Kafka
SCRIPT="kafka-topics.sh"
if ! command -v "$SCRIPT" >/dev/null 2>&1; then
  SCRIPT="/opt/kafka/bin/kafka-topics.sh"
fi

create_topic() {
  local name="$1"
  echo ">>> Création du topic '$name' (${PARTITIONS} partitions, RF=${RF})"
  "$SCRIPT" --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists --topic "$name" \
    --partitions "$PARTITIONS" --replication-factor "$RF"
}

create_topic retail-products      # référentiel produits (clé = product_id)
create_topic retail-customers     # référentiel clients  (clé = customer_id)
create_topic retail-orders        # événements ORDER_CREATED (clé = order_id)
create_topic retail-order-events  # paiements / annulations (clé = order_id)
create_topic retail-events-dlq    # événements invalides (dead letter queue)

echo ">>> Topics existants :"
"$SCRIPT" --bootstrap-server "$BOOTSTRAP" --list