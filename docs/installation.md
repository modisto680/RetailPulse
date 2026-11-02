# Installation et configuration — RetailPulse

## Prérequis

| Outil | Version | Usage |
| --- | --- | --- |
| Docker (+ Compose v2) | ≥ 24 | environnement local complet |
| Python | ≥ 3.10 | tests et benchmarks depuis l'hôte |
| kubectl + cluster (minikube/kind) | ≥ 1.29 | déploiement Kubernetes |

## 1. Environnement local (Docker Compose)

```bash
cp .env.example .env      # adapter le mot de passe PostgreSQL si besoin
docker compose up --build
```

Séquencement automatique (healthchecks + `depends_on`) :

1. `kafka` démarre en mode KRaft et devient sain ;
2. `kafka-init` crée les 5 topics (Job idempotent) ;
3. `postgres` démarre et exécute `postgresql/init/01_schema.sql` ;
4. `collector` collecte DummyJSON (produits + clients) vers Kafka, puis se termine ;
5. `reference-loader` charge les référentiels dans PostgreSQL (upsert), puis se termine ;
6. `producer` démarre la simulation continue ;
7. `spark` consomme et agrège ;
8. `dashboard` est accessible sur http://localhost:8501.

Vérifications :

```bash
docker compose ps
docker compose logs collector | tail -5      # résumé de collecte (volumes)
docker compose exec postgres psql -U retail -d retailpulse \
  -c "SELECT COUNT(*) FROM products;"       # référentiels chargés
docker compose logs -f spark                # batches et upserts
```

Arrêt et nettoyage :

```bash
docker compose stop            # arrêt propre (SIGTERM, flush producteur)
docker compose down            # suppression des conteneurs (volumes conservés)
docker compose down -v          # remise à zéro complète (data, checkpoint)
```

## 2. Déploiement Kubernetes

Voir `k8s/README.md` (construction/versionnement des images, génération de la
ConfigMap du schéma, Secret, ordre d'application des manifestes, exposition
du dashboard en NodePort 30085, vérifications).

## 3. Configuration

Toutes les variables sont documentées dans `.env.example` (Kafka, PostgreSQL,
DummyJSON, simulation, Spark, dashboard). Points sensibles :

- `POSTGRES_PASSWORD` : ne jamais committer `.env` (déjà exclu via `.gitignore`) ;
- `PRODUCER_DUPLICATE_RATE` : injecte des doublons volontaires pour démontrer
  la déduplication (0 par défaut) ;
- `SPARK_WATERMARK_DELAY` / `SPARK_WINDOW_DURATION` : paramètres du traitement ;
- `SPARK_CHECKPOINT_DIR` : chemin du checkpoint (PVC en Kubernetes, volume
  Docker en local) — **ne pas supprimer** sous peine de retraiter depuis le
  début (Kafka conserve, la dédup évite le double comptage).

## 4. Tests et benchmarks

- `tests/README.md` — scénarios de résilience et scripts de vérification ;
- `benchmark/README.md` — protocole de mesure (débit, latence, ressources).

## 5. Dépannage rapide

| Symptôme | Cause probable | Action |
| --- | --- | --- |
| Aucun agrégat n'apparaît | référentiels vides | vérifier les logs `collector` / `reference-loader` |
| Spark : erreurs de connexion Kafka | listener/hostname | vérifier `KAFKA_BOOTSTRAP_SERVERS` (kafka:29092) |
| Doublons dans les agrégats | checkpoint supprimé + watermark dépassé | rejeu complet et clé UNIQUE ; vérifier le PVC |
| Dashboard « indisponible » | PostgreSQL arrêté | `docker compose ps` puis relancer `postgres` |
