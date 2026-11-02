# Tests et scénarios de validation — RetailPulse

Chaque scénario du cahier des charges (§13) est couvert. Les tests Python
publient des événements conformes au contrat et vérifient les résultats
dans PostgreSQL.

## Prérequis

Plateforme démarrée (`docker compose up --build` dans le dépôt racine),
puis depuis ce répertoire :

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## Scénarios automatisés

| Script | Scénario (§13) | Attendu |
| --- | --- | --- |
| `test_failed_payment.py` | Paiement échoué | le CA n'augmente pas ; statut `PAYMENT_FAILED` |
| `test_duplicate.py` | Doublon (même `event_id` x2) | compté UNE fois (dédup Spark) |
| `test_late_event.py` | Événement tardif (T-3min / T-30min) | dans le watermark => fenêtre mise à jour ; hors watermark => abandonné |

```bash
# Pour des deltas exacts, arrêter la simulation pendant les tests :
docker compose stop producer

python test_failed_payment.py   # exit 0 = réussi
python test_duplicate.py
python test_late_event.py
```

## Scénarios manuels (opérations sur les composants)

### 1. Flux nominal

```bash
docker compose up --build
# messages circulent : kafka-console-consumer (voir README racine)
# les agrégats apparaissent :
docker compose exec postgres psql -U retail -d retailpulse \
  -c "SELECT category, order_count, total_revenue FROM sales_aggregates ORDER BY updated_at DESC LIMIT 5;"
```

### 2. Reprise Spark (checkpoint)

```bash
docker compose stop spark          # arrêt
# ... laisser le producteur tourner 1-2 minutes (messages accumulés dans Kafka)
docker compose start spark          # relance avec le MÊME volume de checkpoint
# Attendu : reprise depuis les offsets sauvegardés ; les messages publiés
# pendant l'arrêt sont traités ; AUCUN double comptage (vérifier la somme du CA
# avant/après via le dashboard ou une requête SQL).
```

### 3. Pod indisponible (Kubernetes)

```bash
kubectl -n retailpulse delete pod -l app.kubernetes.io/name=spark
kubectl -n retailpulse get pods -w   # le Deployment recrée le Pod
```

### 4. PostgreSQL indisponible

```bash
docker compose stop postgres         # Spark log des erreurs de connexion
sleep 60
docker compose start postgres        # reprise : Spark reprend au trigger
# suivant, aucune donnée perdue (Kafka conserve ; offsets non commités avant
# écriture réussie). Vérifier l'intégrité : cohérence CA vs orders PAID.
docker compose exec postgres psql -U retail -d retailpulse \
  -c "SELECT SUM(total_amount) FILTER (WHERE status='PAID') FROM orders;"
docker compose exec postgres psql -U retail -d retailpulse \
  -c "SELECT SUM(total_revenue) FROM sales_aggregates;"
# Les deux montants doivent être égaux (aux fenêtres non encore fermées près).
```

### 5. Arrêt propre du producteur

```bash
docker compose stop producer   # SIGTERM => flush Kafka => fin propre (voir logs)
```
