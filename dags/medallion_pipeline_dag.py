from __future__ import annotations

import hashlib
import json
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from airflow.sdk import Asset, Variable, dag, task, task_group

# ── Rutas y constantes 
AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", Path.home() / "airflow"))
DATA_DIR = AIRFLOW_HOME / "data"
SOURCE_DIR = DATA_DIR / "source"      # CSV originales de Kaggle (gitignored)
BRONZE_DIR = DATA_DIR / "bronze"
STAGING_DIR = DATA_DIR / "staging"    # users limpio pero sin enmascarar; nunca llega a silver
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
GOLD_DATASET = "order_items_enriched"
CATALOG_PATH = DATA_DIR / "catalog.duckdb"

# TheLook eCommerce (Kaggle: mustafakeser4/looker-ecommerce-bigquery-dataset)
SOURCE_FILES = {
    "distribution_centers": "distribution_centers.csv",
    "events": "events.csv",
    "inventory_items": "inventory_items.csv",
    "order_items": "order_items.csv",
    "orders": "orders.csv",
    "products": "products.csv",
    "users": "users.csv",
}

GOLD_ASSET = Asset(f"file://{GOLD_DIR / GOLD_DATASET}")


@dag(
    dag_id="medallion_pipeline_dag",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["TP_ING"],
    default_args={"retries": 2, "retry_delay": timedelta(seconds=15)},
    max_active_tasks=4,  # techo de paralelismo: cuida la RAM de WSL
)
def medallion_pipeline_dag():

    # ── Bronze ────────────────────────────────────────────────────────────────

    @task
    def extract(table: str, logical_date: datetime | None = None) -> None:
        """Copia el CSV crudo de `table` a bronze, sin modificarlo.

        Pre:  SOURCE_DIR/SOURCE_FILES[table] existe.
        Post: BRONZE_DIR/{table}/ingested_year=YYYY/ingested_month=MM/ contiene solo ese CSV.
        Inv:  la carpeta destino se borra antes de copiar (overwrite, nunca append).
        """
        pass

    @task
    def validate_bronze() -> None:
        """Compuerta antes de silver: las 7 tablas aterrizaron bien.

        Pre:  corrieron todas las instancias de extract.
        Post: falla el DAG si falta un archivo, está vacío o le faltan columnas esperadas.
        """
        pass

    # ── Silver ────────────────────────────────────────────────────────────────

    @task
    def clean_orders() -> None:
        """Tipado de created/shipped/delivered/returned_at; normaliza casing de status; dedup por order_id.

        Post: SILVER_DIR/orders/ en Parquet; order_id único.
        Inv:  nulls en shipped/delivered/returned_at se aceptan solo si son coherentes con status.
        """
        pass

    @task
    def clean_order_items() -> None:
        """Tipado de sale_price y timestamps; dedup por id.

        Post: SILVER_DIR/order_items/ en Parquet; id único.
        """
        pass

    @task
    def clean_inventory_items() -> None:
        """Tipado de cost y product_retail_price; sold_at nulo = ítem aún en stock; dedup por id.

        Post: SILVER_DIR/inventory_items/ en Parquet; id único.
        """
        pass

    @task
    def clean_products() -> None:
        """Tipado de cost/retail_price; casing de category/department; regla para name/brand nulos.

        Post: SILVER_DIR/products/ en Parquet; id único.
        """
        pass

    @task
    def clean_distribution_centers() -> None:
        """Tipado de latitude/longitude; dedup por id.

        Post: SILVER_DIR/distribution_centers/ en Parquet; id único.
        """
        pass

    @task
    def clean_events() -> None:
        """Tabla más grande. Tipado de created_at; user_id nulo = sesión anónima (se conserva).

        Post: SILVER_DIR/events/ en Parquet; id único; sin ip_address (PII sin uso analítico).
        """
        pass

    @task
    def clean_users() -> None:
        """Tipado de age/created_at; casing de city/state/country; dedup por id. Todavía con PII.

        Post: STAGING_DIR/users/ en Parquet (fuera de silver).
        """
        pass

    @task
    def mask_pii() -> None:
        """Hashea email con SHA-256 + salt; descarta first_name, last_name y street_address.

        Pre:  Variable PII_SALT definida; se lee acá adentro, nunca a nivel módulo.
        Post: SILVER_DIR/users/ sin PII en claro; STAGING_DIR/users/ borrado.
        Inv:  mismo email + mismo salt → mismo hash (joinable, no reversible sin el salt).
        """
        pass

    @task_group(group_id="silver")
    def silver():
        clean_orders()
        clean_order_items()
        clean_inventory_items()
        clean_products()
        clean_distribution_centers()
        clean_events()
        clean_users() >> mask_pii()

    @task
    def validate_silver() -> None:
        """Compuerta antes de gold: unicidad de PKs e integridad referencial entre tablas.

        Pre:  todas las tablas silver escritas.
        Post: loggea huérfanos de order_items → orders/products/users/inventory_items
              y de products → distribution_centers; falla si hay PKs duplicadas.
        """
        pass

    # ── Gold ──────────────────────────────────────────────────────────────────

    @task(outlets=[GOLD_ASSET])
    def build_gold(logical_date: datetime | None = None) -> None:
        """Join order_items + orders + products + users (hasheado) + distribution_centers.

        Pre:  silver validado; ventana = mes calendario de logical_date sobre orders.created_at.
        Post: GOLD_DIR/{GOLD_DATASET}/year=YYYY/month=MM/ solo con ítems de órdenes de ese mes.
        Inv:  se sobreescribe únicamente esa partición; grano = un ítem de orden.
        """
        pass

    @task
    def validate_gold(logical_date: datetime | None = None) -> None:
        """Chequea grano único (order_items.id) y loggea conteo de la partición.

        Post: conteo loggeado → evidencia del test de idempotencia (dos corridas, mismo número).
        """
        pass

    @task
    def register_duckdb() -> None:
        """Expone gold en DuckDB con CREATE OR REPLACE VIEW sobre read_parquet(hive_partitioning=true).

        Post: CATALOG_PATH con la vista actualizada.
        """
        pass

    @task
    def finops_report() -> None:
        """Corre EXPLAIN ANALYZE full scan vs. pruned y guarda ambas salidas.

        Post: reporte en DATA_DIR con archivos y filas leídas por cada query.
        """
        pass

    # ── Dependencias ──────────────────────────────────────────────────────────

    (
        extract.expand(table=list(SOURCE_FILES))
        >> validate_bronze()
        >> silver()
        >> validate_silver()
        >> build_gold()
        >> validate_gold()
        >> [register_duckdb(), finops_report()]
    )


medallion_pipeline_dag()