#!/usr/bin/env python
"""Test — Scénario « Événement tardif » (cahier des charges §13).

Manipulation : publier deux PAYMENT_COMPLETED avec des horodatages anciens :
  1) un événement daté de ~3 minutes (EN DÉDANS du watermark de 10 min)
     => il met à jour la fenêtre de 5 minutes correspondante ;
  2) un événement daté de ~30 minutes (AU-DELÀ du watermark)
     => il est abandonné et ne compte pas dans le CA.

Résultat attendu : comportement du watermark observé et documenté.

Usage (plateforme démarrée) : python tests/test_late_event.py
"""

import sys

import psycopg2

from common import (PG, TOPIC_EVENTS, TOPIC_ORDERS, make_order_event,
                    make_payment_event, make_producer, past_iso, pick_customer,
                    pick_product, publish, scalar, wait_for)


def main() -> int:
    print("== Test : événements tardifs vs watermark (10 minutes) ==")

    with psycopg2.connect(**PG) as conn:
        with conn.cursor() as cur:
            product_id, category, price = pick_product(cur)
            customer_id = pick_customer(cur)

    revenue_before = scalar(
        "SELECT COALESCE(SUM(total_revenue), 0) FROM sales_aggregates") or 0.0
    amount = round(price, 2)

    # Cas 1 : tardif mais DANS le watermark (3 minutes).
    order1 = make_order_event(f"test-{product_id}-late3", customer_id,
                              product_id, category, price, timestamp=past_iso(3))
    payment1 = make_payment_event("PAYMENT_COMPLETED", order1,
                                  timestamp=past_iso(3))
    # Cas 2 : hors watermark (30 minutes).
    order2 = make_order_event(f"test-{product_id}-late30", customer_id,
                              product_id, category, price, timestamp=past_iso(30))
    payment2 = make_payment_event("PAYMENT_COMPLETED", order2,
                                  timestamp=past_iso(30))

    producer = make_producer()
    for ev in (order1, payment1, order2, payment2):
        topic = TOPIC_ORDERS if ev["event_type"] == "ORDER_CREATED" else TOPIC_EVENTS
        publish(producer, topic, ev["order_id"], ev)
    producer.flush(15)
    print(f"Publié : PAYMENT_COMPLETED à T-3min ({amount} USD) et à T-30min ({amount} USD)")

    ok1 = wait_for(
        "fenêtre T-5min..T-0 contenant le paiement à T-3min",
        lambda: (scalar(
            "SELECT COALESCE(SUM(total_revenue), 0) FROM sales_aggregates "
            "WHERE window_start > NOW() - INTERVAL '10 minutes'",
        ) or 0.0) > 0,
    )

    # Après stabilisation, le CA ne doit avoir augmenté qu'UNE fois.
    ok2 = wait_for(
        "CA augmenté d'exactement UN montant (l'événement hors watermark abandonné)",
        lambda: abs((scalar(
            "SELECT COALESCE(SUM(total_revenue), 0) FROM sales_aggregates") or 0.0
        ) - revenue_before - amount) < 0.005,
        timeout=60,
    )

    revenue_after = scalar(
        "SELECT COALESCE(SUM(total_revenue), 0) FROM sales_aggregates") or 0.0
    print(f"  CA avant : {revenue_before:.2f} | après : {revenue_after:.2f} | "
          f"attendu : +{amount:.2f}")
    print("  Interprétation : l'événement à T-3min a mis à jour sa fenêtre "
          "(encore ouverte, watermark non atteint) ; l'événement à T-30min a "
          "été écarté par le watermark.")
    print("\n==> TEST RÉUSSI ✅" if (ok1 and ok2) else "\n==> ÉCHEC ❌")
    return 0 if (ok1 and ok2) else 1


if __name__ == "__main__":
    sys.exit(main())
