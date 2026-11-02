# Tests, résilience et analyse — RetailPulse

Ce dossier répond aux §13 (tests) et §12 (fiabilité) du cahier des charges.
Les commandes exécutables sont dans `tests/README.md` ; ce document structure
l'analyse et les résultats à consigner pour la soutenance.

## 1. Matrice des scénarios

| # | Scénario | Manipulation | Résultat attendu | Où l'observer | Statut |
| --- | --- | --- | --- | --- | --- |
| 1 | Flux nominal | `docker compose up --build` | messages → agrégats dans PostgreSQL | `sales_aggregates`, dashboard | à consigner |
| 2 | Paiement échoué | `python tests/test_failed_payment.py` | CA inchangé, statut `PAYMENT_FAILED` | test + `orders.status` | à consigner |
| 3 | Doublon | `python tests/test_duplicate.py` | compté une seule fois | delta du CA == montant x1 | à consigner |
| 4 | Reprise Spark | stop/start du service `spark` | reprise checkpoint, pas de double comptage | logs offsets, CA avant/après | à consigner |
| 5 | Pod indisponible | `kubectl delete pod -l app.kubernetes.io/name=spark` | recréation par le Deployment, reprise | `kubectl get pods -w` | à consigner |
| 6 | PostgreSQL indisponible | stop/start `postgres` | erreurs tracées, reprise, intégrité | logs Spark + requêtes | à consigner |
| 7 | Événement tardif | `python tests/test_late_event.py` | dans le watermark → fenêtre mise à jour ; hors → abandonné | `sales_aggregates` par fenêtre | à consigner |

## 2. Analyse attendue par scénario

1. **Flux nominal** — décrire le chemin complet d'une commande (Kafka → Spark
   → PostgreSQL → dashboard) avec un exemple de `order_id` suivi.
2. **Paiement échoué** — expliquer le filtre `event_type` avant agrégation :
   un `PAYMENT_FAILED` met à jour `orders.status` mais n'entre jamais dans le
   calcul du CA.
3. **Doublon** — illustrer les DEUX barrières : `dropDuplicates(event_id)`
   (Spark, dans le watermark) et la clé `(category, window_start, window_end)`
   (base). Expliquer pourquoi la deuxième protège aussi le rejeu au-delà du
   watermark.
4. **Reprise Spark** — montrer dans les logs la reprise d'offsets depuis le
   checkpoint ; comparer le CA avant/après redémarrage : la somme reste
   cohérente car l'upsert écrase (mode update) au lieu d'additionner.
5. **Pod indisponible** — mesurer le temps de recréation (readiness) et
   l'impact (aucune donnée perdue : Kafka conserve, PVC checkpoint).
6. **PostgreSQL indisponible** — Spark échoue ses batchs (logs), ne commit pas
   d'offsets ; au rétablissement, les messages non commités sont retraités et
   l'idempotence évite tout doublon. Vérifier `SUM(orders PAID) ≈ SUM(sales)`.
7. **Événement tardif** — relier le comportement au watermark de 10 min : la
   fenêtre T-3min est encore ouverte (mise à jour), la fenêtre T-30min est
   fermée (événement écarté).

## 3. Fiabilité et cohérence (§12)

- Identification des erreurs : `event_id` (unicité de l'événement) ≠
  `order_id` (cycle de commande) ≠ identifiants de référentiel (`product_id`,
  `customer_id`).
- Traçabilité des invalides : topic `retail-events-dlq` + logs (aucun
  message silencieusement perdu).
- Arrêt propre : producteur (SIGTERM → flush), Spark (awaitAnyTermination,
  checkpoint), Kubernetes (grace period 40 s).
- Idempotence démontrée du stockage : rejouer le reference loader et redémarrer
  Spark ne duplique aucune donnée (upsert partout).

## 4. Consigner les résultats

Remplir la colonne « Statut » et joindre les captures : dashboard final,
requêtes de vérification, extraits de logs (reprise d'offsets), sorties des
scripts de test (exit 0), rapports JSON du benchmark.
