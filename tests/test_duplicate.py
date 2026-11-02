#!/usr/bin/env python
"""Test — Scénario « Doublon » (cahier des charges §13).

Manipulation : publier DEUX FOIS le même event_id (PAYMENT_COMPLETED).
Résultat attendu : l'événement n'est comptabilisé qu'UNE SEULE fois
(déduplication Spark par event_id + watermark, défense en profondeur par
la clé UNIQUE(category, window_start, window_end) en base).

Usage (plateforme démarrée) :
    python tests/test_duplicate.py

Remarque : arrêter le producteur pendant le test pour un delta exact
(docker compose stop producer) — sinon le CA inclut l'activité simulée.
"""

import sys

import psycopg2

from common import (PG, TOPIC_EVENTS, TOPIC_ORDERS, make_order_event,
                    make_payment_event, make_producer, pick_customer,
                    pick_product, publish, scalar, wait_for)


def main() -> int:
    print("== Test : un event_id rejoué n'est comptabilisé qu'une fois ==")

    with psycopg2.connect(**PG) as conn:
        with conn.cursor() as cur:
            product_id, category, price = pick_product(cur)
            customer_id = pick_customer(cur)

    revenue_before = scalar(
        "SELECT COALESCE(SUM(total_revenue), 0) FROM sales_aggregates") or 0.0

    order = make_order_event(f"test-{product_id}-dup", customer_id,
                             product_id, category, price)
    payment = make_payment_event("PAYMENT_COMPLETED", order)

    producer = make_producer()
    # Publication du même événement (MÊME event_id) DEUX FOIS.
    publish(producer, TOPIC_ORDERS, order["order_id"], order)
    publish(producer, TOPIC_EVENTS, order["order_id"], payment)
    publish(producer, TOPIC_EVENTS, order["order_id"], payment)  # doublon
    producer.flush(15)
    print(f"Publié : ORDER_CREATED + PAYMENT_COMPLETED x2 "
          f"(event_id {payment['event_id']}, montant {order['amount']} USD)")

    ok = wait_for(
        f"commande {order['order_id']} persistée avec le statut PAID",
        lambda: scalar(
            "SELECT 1 FROM orders WHERE order_id = %s AND status = 'PAID'",
            (order["order_id"],)) == 1,
    )
    if not ok:
        return 1

    # Laisse le temps au doublon d'arriver et d'être écarté par dropDuplicates.
    ok = wait_for(
        "CA stabilisé (delta == montant, compté une seule fois)",
        lambda: abs((scalar(
            "SELECT COALESCE(SUM(total_revenue), 0) FROM sales_aggregates") or 0.0
        ) - revenue_before - order["amount"]) < 0.005,
        timeout=45,
    )
    if not ok:
        return 1

    revenue_after = scalar(
        "SELECT COALESCE(SUM(total_revenue), 0) FROM sales_aggregates") or 0.0
    print(f"  CA avant : {revenue_before:.2f} | après : {revenue_after:.2f} | "
          f"attendu : +{order['amount']:.2f} (UNE fois, pas deux)")
    print("\n==> TEST RÉUSSI ✅" if abs(revenue_after - revenue_before - order["amount"]) < 0.005
          else "\n==> ÉCHEC ❌")
    return 0 if abs(revenue_after - revenue_before - order["amount"]) < 0.005 else 1


if __name__ == "__main__":
    sys.exit(main())
