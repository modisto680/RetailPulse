# RetailPulse 🛒⚡

**Plateforme distribuée de traitement des données e-commerce en quasi-temps réel.**

Projet de synthèse — Master 2 Data, Sup de Vinci — Module *Données distribuées*.

RetailPulse collecte les données de référence depuis l'API publique [DummyJSON](https://dummyjson.com/docs),
simule une activité commerciale continue (commandes et paiements), transporte les événements via
**Apache Kafka (mode KRaft)**, les traite avec **Spark Structured Streaming** (validation, déduplication,
enrichissement, agrégations par fenêtres de 5 minutes), persiste les résultats dans **PostgreSQL** et les
expose dans un **tableau de bord Streamlit**. L'ensemble est conteneurisé avec Docker et déployable sur
**Kubernetes**.

---

## 🏗️ Architecture fonctionnelle

```
┌──────────────┐   ┌─────────────────┐   ┌──────────────────────────────┐
│  DummyJSON   │──▶│  API COLLECTOR  │──▶│  Apache Kafka (KRaft)       │
│ products/    │   │ (Python, Job)   │   │  retail-products             │
│ users/       │   │ collecte        │   │  retail-customers            │
└──────────────┘   │ validation      │   └──────────────┬───────────────┘
                   │ normalisation   │                  │
                   └─────────────────┘   ┌──────────────┴───────────────┐
                                         │  PRODUCTEUR (Python)         │
                                         │  simulation commandes +     │◀── PostgreSQL (référentiels)
                                         │  paiements (continue)       │
                                         └──────────────┬───────────────┘
                                                        │ retail-orders / retail-order-events
                                                        ▼
                                         ┌──────────────────────────────┐
                                         │  SPARK STRUCTURED STREAMING  │
                                         │  filtrage PAYMENT_COMPLETED  │
                                         │  déduplication event_id      │
                                         │  enrichissement produits     │
                                         │  fenêtres 5 min / catégorie  │
                                         └──────────────┬───────────────┘
                                                        │ upsert (checkpoint persistant)
                                                        ▼
┌──────────────┐   ┌─────────────────┐   ┌──────────────────────────────┐
│  DASHBOARD   │◀──│   PostgreSQL    │◀──│  orders / sales_aggregates   │
│  Streamlit   │   │                 │   │  (via foreachBatch JDBC)     │
└──────────────┘   └─────────────────┘   └──────────────────────────────┘
```

> Schémas détaillés (fonctionnel + Kubernetes) et justifications : [docs/architecture.md](docs/architecture.md)

## 🚀 Démarrage rapide (Docker Compose)

```bash
cp .env.example .env          # adapter si besoin (mot de passe PostgreSQL...)
docker compose up --build     # démarre toute la plateforme
```

1. **Kafka** démarre en mode KRaft, les topics sont créés automatiquement (`kafka-init`).
2. **PostgreSQL** crée le schéma au premier démarrage (`postgresql/init/01_schema.sql`).
3. Le **collecteur** interroge DummyJSON et publie les référentiels dans Kafka, puis se termine.
4. Le **reference loader** charge les référentiels dans PostgreSQL (upsert idempotent), puis se termine.
5. Le **producteur** démarre la simulation (commandes + paiements, 0,5 à 2 s d'intervalle).
6. **Spark** consomme les topics, déduplique, enrichit et agrège par fenêtres de 5 minutes.

Dashboard : <http://localhost:8501> — mise à jour toutes les ~10 secondes.

Vérifications utiles :

```bash
docker compose ps                          # état des services
docker compose exec postgres psql -U retail -d retailpulse -c "SELECT COUNT(*) FROM products;"
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic retail-orders --from-beginning --max-messages 3
docker compose logs -f spark
```

Déploiement **Kubernetes** : voir [docs/installation.md](docs/installation.md) (namespace `retailpulse`,
StatefulSets Kafka/PostgreSQL, Jobs, Deployments, PVC de checkpoint, probes, NodePort).

## 📁 Structure du dépôt

```
retailpulse/
├── docker-compose.yml          # environnement local complet
├── .env.example                # variables d'environnement documentées
├── kafka/topics.sh             # création des topics (partitions, RF)
├── collector/                  # collecteur DummyJSON -> Kafka (Job)
├── reference_loader/           # topics de référence -> PostgreSQL (Job, upsert)
├── producer/                   # simulation commerciale continue (Deployment)
├── spark-streaming/            # Spark Structured Streaming (Deployment)
├── postgresql/init/            # schéma SQL : tables, contraintes, index
├── dashboard/                  # tableau de bord Streamlit
├── k8s/                        # manifestes Kubernetes (namespace retailpulse)
├── tests/                     # scénarios de tests du cahier des charges
├── benchmark/                  # mesure de débit, latence et ressources
└── docs/                       # architecture, installation, tests, benchmark
```

## 📦 Règles métier clés

| Règle | Implémentation |
| --- | --- |
| Seuls les `PAYMENT_COMPLETED` comptent dans le chiffre d'affaires | filtre Spark sur `event_type` avant agrégation |
| Un `event_id` rejoué ne doit pas être compté deux fois | `dropDuplicates(["event_id"])` + watermark + clé UNIQUE en base |
| Montant = total **payé** (somme quantité × prix unitaire), devise USD | contrat d'événement documenté (`docs/architecture.md`) |
| Une commande annulée **après** paiement | hors périmètre de la simulation : le producteur émet au plus un événement terminal par commande (règle de compensation définie dans l'architecture) |
| Agrégats rejouables sans doublons | `INSERT ... ON CONFLICT (category, window_start, window_end) DO UPDATE` |

## ✅ Critère d'acceptation

> Une commande simulée traverse Kafka et Spark, est persistée dans PostgreSQL et devient visible sur le
tableau de bord **sans intervention manuelle sur les données**.

Scénarios de validation (flux nominal, paiement échoué, doublon, reprise Spark, pod indisponible,
PostgreSQL indisponible, événement tardif) : [docs/tests-et-resilience.md](docs/tests-et-resilience.md).

Mesures de performance (débit, latence, ressources, 2 expériences minimum) : [docs/benchmark.md](docs/benchmark.md).

## 🗺️ Correspondance avec le cahier des charges

| Livrable | Emplacement |
| --- | --- |
| Dépôt Git organisé, README, installation | ce dépôt + `docs/installation.md` |
| Dossier d'architecture, schémas, modèle de données | `docs/architecture.md` |
| Collecte / référentiels DummyJSON | `collector/`, `reference_loader/` |
| Kafka KRaft, topics, producteur, contrat JSON | `kafka/`, `producer/`, `docker-compose.yml`, `k8s/` |
| Traitement Spark (filtrage, dédup, enrichissement, fenêtres, checkpoint) | `spark-streaming/` |
| PostgreSQL (schéma, contraintes, index, upsert) | `postgresql/init/01_schema.sql` |
| Tableau de bord | `dashboard/` |
| Docker / Kubernetes | `Dockerfile` de chaque composant + `k8s/` |
| Tests, résilience, benchmarks | `tests/`, `benchmark/`, `docs/` |

## 🧑‍🤝‍🧑 Contributeurs

Projet en binôme — contributions identifiables dans l'historique Git.

- **[Membre 1]** — à compléter
- **[Membre 2]** — à compléter

## ⚠️ Limites connues

- `PAYMENT_FAILED` / `ORDER_CANCELLED` n'entraînent aucune compensation du chiffre d'affaires
  (aucun paiement ne peut être annulé après avoir été complété — règle définie et justifiée).
- La déduplication Spark repose sur un watermark de 10 minutes : un doublon rejoué **au-delà** de cette
  fenêtre serait filtré uniquement par la contrainte UNIQUE de la base (défense en profondeur).
- Le détail des lignes de commande (items) n'est pas persisté dans une table dédiée : choix justifié dans
  `docs/architecture.md` (les items restent dans le payload des événements et les agrégats).
- Un seul broker Kafka (RF = 1) : adapté aux ressources pédagogiques, documenté dans `kafka/topics.sh`.
