#!/usr/bin/env python
"""RetailPulse — Mesure de performance (débit, latence, ressources).

Méthode :
    1. Publication de commandes à débit constant (events/s paramétrable)
       directement dans Kafka, conformément au contrat d'événement ;
    2. Débit aval = nombre d'événements devenus visibles dans `orders`
       par seconde (relevé en fin de mesure) ;
    3. Latence de bout en bout = délai entre publication Kafka et
       visibilité en base, mesurée par échantillonnage (chaque N-ième
       commande est sondée jusqu'à apparaître dans `orders`) ;
    4. Les ressources CPU/RAM se relèvent avec `docker stats` ou
       `kubectl top pods` (voir benchmark/README.md).

Usage :
    python benchmark.py --rate 20 --duration 60 --sample 10

Résultat : rapport console + fichier JSON horodaté dans ce répertoire.
"""

import argparse
import json
import os
import statistics
import sys
import time
import uuid
from datetime import datetime, timezone

import psycopg2

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tests"))
from common import (PG, TOPIC_EVENTS, TOPIC_ORDERS, make_order_event,  # noqa: E402
                    make_payment_event, make_producer, pick_customer,
                    pick_product, publish, scalar)


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark RetailPulse")
    parser.add_argument("--rate", type=float, default=10.0,
                        help="événements (commandes) par seconde à produire")
    parser.add_argument("--duration", type=float, default=60.0,
                        help="durée de production en secondes")
    parser.add_argument("--sample", type=int, default=10,
                        help="sonder 1 commande sur N pour la latence")
    parser.add_argument("--output", default=None, help="fichier JSON de sortie")
    args = parser.parse_args()

    print(f"Benchmark : {args.rate} cmd/s pendant {args.duration:.0f}s "
          f"(échantillon latence : 1/{args.sample})")

    with psycopg2.connect(**PG) as conn:
        with conn.cursor() as cur:
            product_id, category, price = pick_product(cur)
            customer_id = pick_customer(cur)

    orders_before = scalar("SELECT COUNT(*) FROM orders") or 0

    producer = make_producer()
    interval = 1.0 / args.rate if args.rate > 0 else 0.0
    total = int(args.rate * args.duration)
    latencies = []
    sampled = {}  # order_id -> t_publish
    t_start = time.time()

    for i in range(total):
        order = make_order_event(f"bench-{uuid.uuid4().hex[:10]}", customer_id,
                                  product_id, category, price)
        payment = make_payment_event("PAYMENT_COMPLETED", order)
        publish(producer, TOPIC_ORDERS, order["order_id"], order)
        publish(producer, TOPIC_EVENTS, order["order_id"], payment)
        producer.poll(0)

        if i % args.sample == 0:
            sampled[order["order_id"]] = time.time()

        # cadence : attendre la fin du créneau courant
        target = t_start + (i + 1) * interval
        while time.time() < target:
            time.sleep(0.001)

    producer.flush(30)
    t_end_publish = time.time()
    print(f"Produit : {total} commandes en {t_end_publish - t_start:.1f}s "
          f"({total / (t_end_publish - t_start):.1f} cmd/s effectives)")

    # Attente de consommation complète (visibilité dans orders).
    deadline = time.time() + max(120, args.duration)
    while time.time() < deadline:
        current = scalar("SELECT COUNT(*) FROM orders") or 0
        if current - orders_before >= total:
            break
        time.sleep(2)

    t_end_consume = time.time()
    consumed = (scalar("SELECT COUNT(*) FROM orders") or 0) - orders_before

    # Latences de bout en bout sur les commandes échantillonnées.
    for order_id, t_publish in sampled.items():
        deadline = time.time() + 60
        while time.time() < deadline:
            if scalar("SELECT 1 FROM orders WHERE order_id = %s", (order_id,)) == 1:
                latencies.append(time.time() - t_publish)
                break
            time.sleep(0.5)

    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "params": {"rate": args.rate, "duration": args.duration,
                   "sample": args.sample},
        "produced": total,
        "consumed": consumed,
        "production_throughput": round(total / (t_end_publish - t_start), 2),
        "pipeline_throughput": round(consumed / max(t_end_consume - t_start, 1e-9), 2),
        "latency_samples": len(latencies),
        "latency_avg_s": round(statistics.mean(latencies), 3) if latencies else None,
        "latency_median_s": round(statistics.median(latencies), 3) if latencies else None,
        "latency_p95_s": round(sorted(latencies)[int(len(latencies) * 0.95) - 1], 3)
            if latencies else None,
    }

    print("\n===== RAPPORT =====")
    for key, value in report.items():
        print(f"{key:24}: {value}")

    output = args.output or f"results-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}.json"
    with open(output, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False)
    print(f"\nRapport sauvegardé : {output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
