-- ============================================================
-- RetailPulse — Schéma PostgreSQL
-- Exécuté automatiquement au premier démarrage du conteneur
-- (docker-entrypoint-initdb.d) ou manuellement : psql -f 01_schema.sql
-- ============================================================

-- --------------------------------------------------------------
-- Table PRODUCTS : catalogue produits (référentiel DummyJSON)
-- Alimentée par le Reference Loader (upsert idempotent par product_id).
-- --------------------------------------------------------------
CREATE TABLE IF NOT EXISTS products (
    product_id          INTEGER PRIMARY KEY,     -- identifiant DummyJSON unique
    title               VARCHAR(255) NOT NULL,
    description         TEXT,
    category            VARCHAR(100) NOT NULL,   -- dimension d'agrégation Spark
    brand               VARCHAR(100),
    price               NUMERIC(12,2) NOT NULL CHECK (price >= 0),
    discount_percentage NUMERIC(5,2),
    rating              NUMERIC(3,2),
    stock               INTEGER NOT NULL DEFAULT 0,
    thumbnail           TEXT,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Justification : le dashboard filtre/groupe par catégorie (top catégories,
-- répartition du CA). L'index sert les agrégations du tableau de bord.
CREATE INDEX IF NOT EXISTS idx_products_category ON products (category);

-- --------------------------------------------------------------
-- Table CUSTOMERS : référentiel clients
-- Alimentée par le Reference Loader (upsert idempotent par customer_id).
-- --------------------------------------------------------------
CREATE TABLE IF NOT EXISTS customers (
    customer_id  INTEGER PRIMARY KEY,           -- identifiant DummyJSON unique
    first_name   VARCHAR(100) NOT NULL,
    last_name    VARCHAR(100) NOT NULL,
    email        VARCHAR(255) NOT NULL,
    age          INTEGER CHECK (age IS NULL OR age >= 0),
    city         VARCHAR(100),
    country      VARCHAR(100),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- Choix documenté : email SANS contrainte UNIQUE. DummyJSON peut contenir
-- des doublons d'e-mails ; l'unicité est portée par customer_id (clé source).

-- --------------------------------------------------------------
-- Table ORDERS : commandes simulées
-- Alimentée par le job Spark Structured Streaming :
   1) upsert à chaque ORDER_CREATED (statut PENDING) ;
   2) mise à jour du statut à chaque événement de paiement.
-- (composant identifié : spark-streaming/spark_job.py, foreachBatch)
-- --------------------------------------------------------------
CREATE TABLE IF NOT EXISTS orders (
    order_id      VARCHAR(64) PRIMARY KEY,       -- généré par le producteur
    customer_id   INTEGER NOT NULL REFERENCES customers (customer_id),
    created_at    TIMESTAMPTZ NOT NULL,          -- event_timestamp du ORDER_CREATED
    status        VARCHAR(32) NOT NULL DEFAULT 'PENDING'
                 CHECK (status IN ('PENDING', 'PAID', 'PAYMENT_FAILED', 'CANCELLED')),
    total_amount  NUMERIC(12,2) NOT NULL CHECK (total_amount >= 0), -- montant payé
    currency      CHAR(3) NOT NULL DEFAULT 'USD',
    item_count    INTEGER NOT NULL DEFAULT 0,
    paid_at       TIMESTAMPTZ,                   -- horodatage du PAYMENT_COMPLETED
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Justification (cahier des charges §9) : filtres du dashboard et diagnostics.
CREATE INDEX IF NOT EXISTS idx_orders_customer   ON orders (customer_id); -- par client
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders (created_at);  -- dernières commandes
CREATE INDEX IF NOT EXISTS idx_orders_status      ON orders (status);     -- commandes payées

-- --------------------------------------------------------------
-- Table SALES_AGGREGATES : résultats analytiques
-- Alimentée par Spark Structured Streaming (fenêtres 5 min / catégorie).
-- Unicité garantie par la clé (category, window_start, window_end) :
-- l'upsert ON CONFLICT DO UPDATE rend le stockage idempotent au rejeu.
-- --------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sales_aggregates (
    category         VARCHAR(100) NOT NULL,
    window_start     TIMESTAMPTZ NOT NULL,
    window_end       TIMESTAMPTZ NOT NULL,
    order_count      BIGINT NOT NULL DEFAULT 0,   -- paiements complétés distincts
    total_quantity   BIGINT NOT NULL DEFAULT 0,   -- articles vendus
    total_revenue    NUMERIC(14,2) NOT NULL DEFAULT 0, -- CA (paiements réussis)
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (category, window_start, window_end)
);

-- Justification : courbe d'évolution du CA par fenêtre (ORDER BY window_start).
CREATE INDEX IF NOT EXISTS idx_sales_window_start ON sales_aggregates (window_start);

-- --------------------------------------------------------------
-- Remarque anti-doublons (cahier des charges §9) :
-- Les événements bruts ne sont PAS persistés dans une table dédiée : la
-- contrainte UNIQUE sur event_id n'est donc pas requise ici. La détection de
-- doublons est assurée en amont par Spark (dropDuplicates + watermark) et la
-- cohérence des agrégats par la clé UNIQUE ci-dessus (défense en profondeur).
-- --------------------------------------------------------------