"""RetailPulse — Tableau de bord Streamlit.

Rôle :
    Visualiser les indicateurs de l'activité commerciale en quasi-temps réel.

Contrainte (cahier des charges §10) :
    L'interface lit UNIQUEMENT les données persistées dans PostgreSQL
    (`sales_aggregates`, `orders`, `products`). Elle ne recalcule AUCUNE
    agrégation métier : tout est produit par le pipeline Spark.

Indicateurs :
    - Chiffre d'affaires total (issu des paiements réussis) ;
    - Nombre total de commandes / commandes payées ;
    - Nombre total d'articles vendus ;
    - Nombre de catégories représentées ;
    - Évolution du CA par fenêtres de 5 minutes ;
    - Répartition du CA par catégorie ;
    - Top 5 catégories ;
    - Dernières commandes (horodatage + statut) ;
    - Date/heure de dernière actualisation et état de disponibilité.

Paramètres (variables d'environnement) :
    POSTGRES_HOST/PORT/DB/USER/PASSWORD, DASHBOARD_REFRESH_SECONDS (déf. 10).
"""

import os
import time

import pandas as pd
import psycopg2
import streamlit as st

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "retailpulse"),
    "user": os.getenv("POSTGRES_USER", "retail"),
    "password": os.getenv("POSTGRES_PASSWORD", "retail"),
}
REFRESH_SECONDS = int(os.getenv("DASHBOARD_REFRESH_SECONDS", "10"))

st.set_page_config(page_title="RetailPulse", page_icon="🛒", layout="wide")

# Auto-actualisation NATIVE (fréquence documentée, 5–15 s) : le script se
# relance lui-même via st.rerun() à la fin de la page (aucune dépendance
# externe — streamlit-autorefresh est abandonné au profit des API natives).


# ---------------------------------------------------------------------------
# Accès PostgreSQL (avec gestion des erreurs de connexion)
# ---------------------------------------------------------------------------
def query(sql: str) -> pd.DataFrame:
    """Exécute une requête en lecture ; DataFrame vide si indisponible."""
    try:
        with psycopg2.connect(**PG) as conn:
            return pd.read_sql(sql, conn)
    except psycopg2.OperationalError as exc:
        st.session_state.setdefault("last_db_error", str(exc))
        return pd.DataFrame()


def scalar(sql: str) -> float:
    df = query(sql)
    return float(df.iloc[0, 0]) if not df.empty else 0.0


# ---------------------------------------------------------------------------
# Récupération des données (aucune agrégation métier recalculée ici)
# ---------------------------------------------------------------------------
kpi_sales = query("""
    SELECT COALESCE(SUM(total_revenue), 0)  AS total_revenue,
           COALESCE(SUM(total_quantity), 0) AS total_quantity,
           COUNT(DISTINCT category)          AS categories
    FROM sales_aggregates
""")

kpi_orders = query("""
    SELECT COUNT(*)                                     AS orders_total,
           COUNT(*) FILTER (WHERE status = 'PAID')       AS orders_paid
    FROM orders
""")

revenue_by_window = query("""
    SELECT window_start, SUM(total_revenue) AS revenue
    FROM sales_aggregates
    GROUP BY window_start
    ORDER BY window_start
""")

revenue_by_category = query("""
    SELECT category, SUM(total_revenue) AS revenue,
           SUM(total_quantity) AS quantity, SUM(order_count) AS order_count
    FROM sales_aggregates
    GROUP BY category
    ORDER BY revenue DESC
""")

recent_orders = query("""
    SELECT order_id, customer_id, created_at, status,
           total_amount, currency, item_count
    FROM orders
    ORDER BY created_at DESC
    LIMIT 10
""")

last_sales_update = scalar("SELECT MAX(updated_at) FROM sales_aggregates")
last_order_update = scalar("SELECT MAX(updated_at) FROM orders")

# ---------------------------------------------------------------------------
# Rendu
# ---------------------------------------------------------------------------
st.title("🛒 RetailPulse — Activité e-commerce en quasi-temps réel")

# État de disponibilité des données
db_ok = "last_db_error" not in st.session_state or not kpi_sales.empty
col_a, col_b, col_c = st.columns([2, 2, 2])
with col_a:
    st.caption(f"Actualisation automatique : toutes les {REFRESH_SECONDS} s")
with col_b:
    if not db_ok:
        st.error("⚠️ PostgreSQL indisponible — nouvelle tentative à la prochaine actualisation.")
    else:
        st.success("✅ PostgreSQL disponible")
with col_c:
    last_update = max(last_sales_update, last_order_update)
    if last_update:
        st.caption(f"Dernière donnée intégrée : **{last_update}** (UTC)")
    else:
        st.caption("Aucune donnée intégrée pour le moment.")

st.divider()

has_sales = not kpi_sales.empty and kpi_sales.iloc[0]["total_revenue"] > 0
if not has_sales:
    st.info(
        "En attente de données : vérifiez que le producteur (simulation) et le "
        "job Spark Structured Streaming sont démarrés. Les agrégats "
        "apparaîtront automatiquement dans quelques dizaines de secondes."
    )

# --- Indicateurs clés ----------------------------------------------------
row = kpi_sales.iloc[0] if not kpi_sales.empty else {"total_revenue": 0, "total_quantity": 0, "categories": 0}
orders = kpi_orders.iloc[0] if not kpi_orders.empty else {"orders_total": 0, "orders_paid": 0}

m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("💰 CA total (USD)", f"{row['total_revenue']:,.2f}",
          help="Somme des paiements réussis (PAYMENT_COMPLETED uniquement)")
m2.metric("🧾 Commandes", f"{orders['orders_total']:,}",
          help="Commandes simulées persistées par Spark")
m3.metric("✅ Commandes payées", f"{orders['orders_paid']:,}",
          help="Commandes dont le paiement est complété")
m4.metric("📦 Articles vendus", f"{row['total_quantity']:,}",
          help="Quantités cumulées issues des agrégats (paiements réussis)")
m5.metric("🏷️ Catégories actives", f"{row['categories']:,}",
          help="Catégories représentées dans les agrégats")

st.divider()

# --- Graphiques ----------------------------------------------------------
chart_col, pie_col = st.columns([3, 2])

with chart_col:
    st.subheader("📈 Évolution du chiffre d'affaires (fenêtres de 5 min)")
    if not revenue_by_window.empty:
        series = revenue_by_window.set_index("window_start")["revenue"]
        st.line_chart(series, height=300)
    else:
        st.write("Pas encore d'historique de fenêtres.")

with pie_col:
    st.subheader("🥧 Répartition du CA par catégorie")
    if not revenue_by_category.empty:
        top = revenue_by_category.set_index("category")["revenue"].head(10)
        st.bar_chart(top, height=300)
    else:
        st.write("Pas encore de répartition par catégorie.")

st.subheader("🏆 Top 5 catégories")
if not revenue_by_category.empty:
    top5 = revenue_by_category.head(5)[
        ["category", "revenue", "quantity", "order_count"]
    ].rename(columns={
        "category": "Catégorie",
        "revenue": "CA (USD)",
        "quantity": "Articles vendus",
        "order_count": "Commandes payées",
    })
    st.dataframe(top5, use_container_width=True, hide_index=True)

st.subheader("🕘 Dernières commandes")
if not recent_orders.empty:
    display = recent_orders.rename(columns={
        "order_id": "Commande", "customer_id": "Client", "created_at": "Créée le (UTC)",
        "status": "Statut", "total_amount": "Montant", "currency": "Devise",
        "item_count": "Articles",
    })
    st.dataframe(display, use_container_width=True, hide_index=True)
else:
    st.write("Aucune commande persistée pour le moment.")

st.divider()
st.caption(
    "RetailPulse — Master 2 Data · Données distribuées · Les indicateurs "
    "respectent les règles métier du pipeline (seuls les paiements complétés "
    "comptent dans le chiffre d'affaires ; montants en USD)."
)

# ---------------------------------------------------------------------------
# Boucle d'auto-actualisation : relance complète du script toutes les N s.
# (Dashboard en lecture seule : aucun widget interactif à interrompre.)
# ---------------------------------------------------------------------------
time.sleep(REFRESH_SECONDS)
st.rerun()
