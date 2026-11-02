#!/usr/bin/env python
"""Test — Scénario « Paiement échoué » (cahier des charges §13).

Manipulation : publier une commande puis un PAYMENT_FAILED.
Résultat attendu : le paiement n'augmente PAS le chiffre d'affaires ;
la commande apparaît avec le statut PAYMENT_FAILED.

Usage (plateforme démarrée) :
    python tests/test_failed_payment.py
"""

import sys

import psycopg2

from common import (PG, TOPIC_EVENTS, TOPIC_ORDERS, make_order_event,
                    make_payment_event, make_producer, pick_customer,
                    pick_product, publish, scalar, wait_for)


def main() -> int:
    print("== Test : PAYMENT_FAILED ne compte pas dans le chiffre d'affaires ==")

    with psycopg2.connect(**PG) as conn:
        with conn.cursor() as cur:
            product_id, category, price = pick_product(cur)
            customer_id = pick_customer(cur)

    revenue_before = scalar(
        "SELECT COALESCE(SUM(total_revenue), 0) FROM sales_aggregates") or 0.0

    order = make_order_event(f"test-{product_id}-failed", customer_id,
                             product_id, category, price)
    failed = make_payment_event("PAYMENT_FAILED", order)

    producer = make_producer()
    publish(producer, TOPIC_ORDERS, order["order_id"], order)
    publish(producer, TOPIC_EVENTS, order["order_id"], failed)
    producer.flush(15)
    print(f"Publié : ORDER_CREATED + PAYMENT_FAILED (commande {order['order_id']}, "
          f"montant {order['amount']} USD)")

    ok = wait_for(
        f"commande {order['order_id']} persistée avec le statut PAYMENT_FAILED",
        lambda: scalar(
            "SELECT 1 FROM orders WHERE order_id = %s AND status = 'PAYMENT_FAILED'",
            (order["order_id"],)) == 1,
    )
    if not ok:
        return 1

    revenue_after = scalar(
        "SELECT COALESCE(SUM(total_revenue), 0) FROM sales_aggregates") or 0.0

    delta = revenue_after - revenue_before
    print(f"  CA avant : {revenue_before:.2f} USD | après : {revenue_after:.2f} USD "
          f"| delta : {delta:.2f} USD")
    if abs(delta) < 0.005:
        print("  [OK] Le paiement échoué n'a PAS augmenté le chiffre d'affaires.")
        print("\n==> TEST RÉUSSI ✅")
        return 0
    print("  [ÉCHEC] Le chiffre d'affaires a varié : un paiement échoué a été "
          "comptabilisé (ou un autre paiement simultané a eu lieu — relancer le "
          "test en arrêtant le producteur).")
    return 1


if __name__ == "__main__":
    sys.exit(main())
