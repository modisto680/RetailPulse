# Dossier d'architecture — RetailPulse

Master 2 Data · Module Données distribuées · Réponse au cahier des charges.

## 1. Schéma fonctionnel

```
                    DONNÉES DE RÉFÉRENCE (DummyJSON)                 ÉVÉNEMENTS SIMULÉS
┌──────────────────┐    ┌─────────────────┐    ┌────────────────┐
│ GET /products    │───▶│ API COLLECTOR   │───▶│ Kafka          │
│ GET /users       │    │ validation      │    │ retail-products│
│ (limit=0)        │    │ normalisation   │    │ retail-customers│
└──────────────────┘    │ retry/backoff  │    └───────┬────────┘
                        └─────────────────┘            │ (consumer group
                        ┌────────────────────┐         │  reference-loader)
                        │ REFERENCE LOADER   │◀────────┘
                        │ upsert PostgreSQL  │
                        │ ON CONFLICT (id)   │
                        └─────────┬──────────┘
                                  │ products / customers
                                  ▼
┌─────────────────────┐   ┌──────────────┐   ┌──────────────────────────────┐
│ PRODUCTEUR          │──▶│ Kafka        │──▶│ SPARK STRUCTURED STREAMING   │
│ charge référentiels │   │ retail-orders│   │ 1. parse + validation (DLQ)  │
│ simule commandes     │   │ retail-order-│   │ 2. watermark 10 min         │
│ + paiements          │   │ events       │   │ 3. dropDuplicates(event_id) │
└─────────────────────┘   └──────────────┘   │ 4. enrich. produits (join)  │
                                              │ 5. fenêtres 5 min/catégorie │
                                              │ 6. foreachBatch -> upsert   │
                                              └──────────────┬───────────────┘
                                                             │ orders, sales_aggregates
                                                             ▼
                                        ┌──────────────────────────────┐
                                        │ PostgreSQL                  │
                                        │ (contraintes, index, upsert)│
                                        └──────────────┬───────────────┘
                                                       │ lectures
                                                       ▼
                                        ┌──────────────────────────────┐
                                        │ Dashboard Streamlit          │
                                        │ (aucune agrégation recalculée)│
                                        └──────────────────────────────┘
```

## 2. Schéma Kubernetes (namespace `retailpulse`)

```
NAMESPACE retailpulse
├── StatefulSet  kafka (KRaft, broker+controller)  + PVC data (5Gi)
├── StatefulSet  postgres                           + PVC data (2Gi)
├── Job          kafka-topics-init  (création topics, idempotent)
├── Job          collector          (DummyJSON -> Kafka)
├── Job          reference-loader   (Kafka -> PostgreSQL)
├── Deployment  producer           (simulation continue)
├── Deployment  spark              + PVC checkpoint (2Gi)
├── Deployment  dashboard          + Service NodePort 30085
├── ConfigMap    retailpulse-config / retailpulse-postgres-init
└── Secret       retailpulse-postgres (jamais en clair dans Git)
```

## 3. Composants : rôles, entrées, sorties, paramètres

| Composant | Fichier | Entrées | Sorties | Paramètres clés |
| --- | --- | --- | --- | --- |
| API Collector | `collector/collector.py` | DummyJSON `/products?limit=0`, `/users?limit=0` | topics `retail-products`, `retail-customers` | timeout, 3 retries + backoff |
| Reference Loader | `reference_loader/reference_loader.py` | topics de référence | tables `products`, `customers` | upsert `ON CONFLICT`, offsets après écriture |
| Producteur | `producer/producer.py` | tables `products`, `customers` | topics `retail-orders`, `retail-order-events` | intervalle 0,5–2 s, taux 85/10/5 % |
| Spark Streaming | `spark-streaming/spark_job.py` | topics `retail-orders` + `retail-order-events`, table `products` | tables `orders`, `sales_aggregates`, topic DLQ | watermark 10 min, fenêtre 5 min, trigger 10 s |
| Dashboard | `dashboard/app.py` | tables PostgreSQL | page web | refresh 10 s |

Chaque composant documente ses paramètres en tête de fichier (docstring) et
dans `.env.example`.

## 4. Contrat d'événement (JSON)

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "event_type": "PAYMENT_COMPLETED",
  "event_timestamp": "2026-11-02T10:15:00.123Z",
  "order_id": "ord-9f2c1a4b8d3e",
  "customer_id": 42,
  "items": [
    {"product_id": 7, "title": "...", "category": "fragrances",
     "quantity": 2, "unit_price": 12.99}
  ],
  "amount": 25.98,
  "currency": "USD",
  "payment_method": "credit_card",
  "reason": null
}
```

- `event_id` : UUID v4, base de la déduplication.
- `event_timestamp` : ISO 8601 UTC avec millisecondes.
- `amount` : **montant payé** = somme(quantité × prix unitaire catalogue),
  prix brut (hors remise), devise USD.
- Types : `ORDER_CREATED`, `PAYMENT_COMPLETED`, `PAYMENT_FAILED`, `ORDER_CANCELLED`.

## 5. Règles métier

1. **Chiffre d'affaires** : seuls les `PAYMENT_COMPLETED` sont comptés.
   Spark filtre sur `event_type` AVANT toute agrégation. Les montants
   `PAYMENT_FAILED`/`ORDER_CANCELLED` ne sont jamais sommés.
2. **Compensation** : le producteur émet **au plus un événement terminal par
   commande**. Un paiement complété ne peut ensuite pas être annulé : aucune
   règle de compensation n'est donc nécessaire, et un `ORDER_CANCELLED`
   n'annule jamais un encaissement. Ce choix est explicite et documenté.
3. **Déduplication** : un `event_id` rejoué est compté une fois (§7 ci-dessous).

## 6. Kafka : topics, partitionnement, clés

| Topic | Partitions | Clé | Justification |
| --- | --- | --- | --- |
| `retail-products` | 3 | `product_id` | même produit = même partition, ordre stable |
| `retail-customers` | 3 | `customer_id` | idem |
| `retail-orders` | 3 | `order_id` | tous les événements d'une commande dans l'ordre |
| `retail-order-events` | 3 | `order_id` | **cohérence ORDER_CREATED → paiement** : le même `order_id` tombe dans la même partition, donc l'ordre est garanti pour le cycle complet |
| `retail-events-dlq` | 3 | clé d'origine | messages invalides, inspection/debug |

- **3 partitions** : compromis pédagogique — parallélisme effectif (3 tâches
  de lecture) et scalable ; suffisant pour un broker unique.
- La clé `order_id` (et non `customer_id`) évite un hot partition client tout
  en garantissant l'ordre au sein de chaque cycle de commande.
- **RF = 1** : un seul broker ; il n'existe aucune autre machine pour héberger
  une copie (documenté dans `kafka/topics.sh` et les manifestes).

## 7. Traitement Spark : justifications

1. **Validation** : `from_json` + contrôle des champs obligatoires ; les
   messages invalides partent dans `retail-events-dlq` (traçabilité) —
   ils sont isolés, jamais perdus silencieusement.
2. **Watermark = 10 minutes** sur `event_timestamp` : borne l'état de
   déduplication et des fenêtres ; accepte un retard raisonnable (réseau,
   reprise) tout en purgeant la mémoire.
3. **Déduplication** : `dropDuplicates(["event_id"])` après le watermark.
   Hypothèse : un doublon est rejoué dans les 10 minutes. Défense en
   profondeur : la clé `UNIQUE(category, window_start, window_end)` des
   agrégats rend l'upsert idempotent.
4. **Événements tardifs** : au-delà du watermark => abandonnés (choix
   documenté) ; en deçà => la fenêtre est recalculée et l'upsert écrase la
   valeur cumulée.
5. **Enrichissement** : join broadcast avec la table `products` (petite
   dimension, ~200 lignes). La catégorie du référentiel prime sur celle de
   l'événement.
6. **Fenêtres de 5 minutes** : agrégats par `window(event_ts, 5 min)` et
   `category` (count distinct `order_id`, quantités, CA).
7. **Écritures** : `foreachBatch` + `INSERT ... ON CONFLICT DO UPDATE`.
   En mode `update`, chaque ligne porte la valeur cumulée de sa fenêtre :
   l'upsert écrase avec la valeur fraîche => idempotence au rejeu.
8. **Checkpoint** : un répertoire par requête (`orders`, `aggregates`, `dlq`),
   sur PVC : reprise exacte (offsets + état) après redémarrage.

## 8. Modèle de données PostgreSQL

Voir `postgresql/init/01_schema.sql`. Tables : `products`, `customers`,
`orders`, `sales_aggregates`.

- **`orders` est alimentée par le job Spark** (ORDER_CREATED = upsert PENDING ;
  événements de paiement = mise à jour du statut) — le composant est
  identifié conformément au cahier des charges §9.
- **Le détail des lignes de commande n'est pas persisté** (choix assumé) :
  les items restent dans le payload des événements (conservés dans Kafka) et
  l'analytique par ligne est portée par `sales_aggregates` (catégorie, fenêtre,
  quantités). Ajouter une table `order_items` serait une extension directe.
- **Index** : `products(category)` (répartition par catégorie),
  `orders(customer_id)`, `orders(created_at)` (dernières commandes),
  `orders(status)` (commandes payées), `sales_aggregates(window_start)`
  (courbe d'évolution).
- **Contrainte `UNIQUE` sur `event_id`** : non requise — les événements bruts
  ne sont pas persistés dans une table dédiée (remarque dans le schéma SQL).

## 9. Fiabilité, cohérence, transactions

| Brique | Garantie | Limite |
| --- | --- | --- |
| Kafka | transport, rétention (rejeu possible) | RF=1 : pas de tolérance au crash du broker |
| Producer Python | `acks`, flush à l'arrêt (SIGTERM) | at-least-once possible après retry |
| Reference loader | at-least-once + upsert => idempotent | — |
| Spark | checkpoint, dédup par event_id, upsert cumulatif | doublons hors watermark filtrés par la clé UNIQUE |
| PostgreSQL | transactions, contraintes, FK | une commande dont le client est absent du référentiel est rejetée et tracée (log) |
| Kubernetes | redémarrage des Deployments, PVC | perte de l'état Spark si le PVC est supprimé |

**Aucun mécanisme isolé ne garantit l'absence de doublons de bout en bout** ;
c'est la combinaison déduplication Spark + contraintes d'unicité + upsert qui
l'assure ici.

## 10. Démarche de réalisation (§16 du cahier des charges)

Cadrage → exploration DummyJSON → référentiels → Kafka KRaft → simulation →
Spark Streaming → PostgreSQL → dashboard → Docker/Kubernetes → validation
(cf. `docs/tests-et-resilience.md` et `docs/benchmark.md`).
