# Benchmark — RetailPulse (§14 du cahier des charges)

Mesure **reproductible** du débit, de la latence et des ressources.

## Méthodologie

- **Débit produit** : commandes/s publiées dans Kafka à cadence constante.
- **Débit aval** : commandes/s devenues visibles dans `orders` (consommées par Spark et persistées).
- **Latence de bout en bout** : écart entre la publication Kafka et la visibilité en base
  (sondage de `orders` par `order_id` sur un échantillon 1/N).
- **Ressources** : `docker stats` (local) ou `kubectl top pods -n retailpulse` (cluster).

## Expériences minimales (2 variations)

### Expérience 1 — variation du débit de production

```bash
# Arrêter le producteur de simulation pour isoler la mesure
docker compose stop producer

# mesure A : débit nominal
python benchmark.py --rate 10 --duration 60 --sample 10
# mesure B : débit élevé
python benchmark.py --rate 50 --duration 60 --sample 10
```

### Expérience 2 — variation des partitions Kafka (ou des ressources CPU/RAM)

```bash
# Recréer les topics avec 6 partitions (idempotent : --if-not-exists ne change
# pas un topic existant ; il faut le recréer après arrêt complet) :
docker compose down
BOOTSTRAP=localhost:9092 KAFKA_NUM_PARTITIONS=6 ./kafka/topics.sh   # après relance
docker compose up -d
python benchmark.py --rate 50 --duration 60 --sample 10

# OU variante ressources : éditer les limits du service spark puis relancer :
#   docker compose up -d --force-recreate spark
docker stats --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}" --no-stream
```

> Sur Kubernetes : `kubectl top pods -n retailpulse`, et modifier les
> requests/limits de `k8s/08-spark.yaml` avant `kubectl apply`.

## Interprétation attendue

Comparer : débit aval vs débit produit (goulot d'étranglement ?), latence
médiane vs p95 (queues ?), CPU Spark vs débit (saturation ?). Proposer des
améliorations : partitions supplémentaires, `maxOffsetsPerTrigger`, batch
JDBC (`executemany` / COPY), ressources du Pod Spark.

Les rapports JSON (`results-*.json`) sont à committer ou joindre au dossier
> de soutenance (ils ne contiennent aucune donnée sensible).
