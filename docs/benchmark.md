# Mesures de performance — RetailPulse (§14)

Protocole exécutable : `benchmark/README.md`. Ce document structure le rapport.

## 1. Métriques et méthode de mesure

| Métrique | Définition | Méthode |
| --- | --- | --- |
| Débit produit | commandes/s publiées | cadence imposée par `benchmark.py --rate` |
| Débit aval | commandes/s persistées | comptage `orders` entre début et fin de mesure |
| Latence bout en bout | publication Kafka → visibilité base | sondage `orders` par `order_id` sur échantillon 1/N (biais de sondage ~0,5 s à déclarer) |
| Ressources | CPU/RAM des composants | `docker stats` / `kubectl top pods` |

## 2. Expériences (minimum deux, reproductibles)

| # | Variable | Configuration A | Configuration B | Comparaison attendue |
| --- | --- | --- | --- | --- |
| 1 | Débit de production | 10 cmd/s, 60 s | 50 cmd/s, 60 s | saturation éventuelle de Spark (goulot) |
| 2 | Partitions Kafka (ou CPU/RAM Spark) | 3 partitions | 6 partitions (ou limits CPU x2) | parallélisme de consommation |

Commandes : voir `benchmark/README.md` (§Expériences).

## 3. Résultats à consigner

Pour chaque configuration, reporter le JSON produit (`results-*.json`) :

| Configuration | Débit produit | Débit aval | Latence médiane | Latence p95 | CPU Spark max | RAM Spark max |
| --- | --- | --- | --- | --- | --- | --- |
| 10 cmd/s, 3 part. | à remplir | à remplir | à remplir | à remplir | à remplir | à remplir |
| 50 cmd/s, 3 part. | | | | | | |
| 50 cmd/s, 6 part. | | | | | | |

## 4. Analyse attendue

- Identifier le goulot d'étranglement (Spark foreachBatch ? JDBC ? Kafka ?).
- Relier la latence au `SPARK_TRIGGER_INTERVAL` (10 s) : la latence médiane
  ne peut pas descendre sous le trigger + une écriture.
- Commenter l'effet des partitions sur le parallélisme de lecture.

## 5. Améliorations proposées

1. Batch JDBC plus large (`executemany` groupé, ou `COPY` PostgreSQL) ;
2. `maxOffsetsPerTrigger` pour lisser les micro-batches ;
3. Partitions supplémentaires si le débit augmente ;
4. Augmenter les ressources du Pod Spark ;
5. Index composites si les requêtes du dashboard deviennent le goulot.
