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
    def extract(table: str, logical_date: datetime | None = None, dag_run=None) -> str:
        """Copia el CSV crudo de `table` a bronze, sin modificarlo.

        Pre:  SOURCE_DIR/SOURCE_FILES[table] existe.
        Post: BRONZE_DIR/{table}/ingested_year=YYYY/ingested_month=MM/ contiene solo ese CSV,
              byte a byte igual al original. Devuelve su ruta (XCom liviano, nunca datos).
        Inv:  la carpeta destino se borra antes de copiar (overwrite, nunca append).
              La fecha de ingesta sale de la corrida, no del reloj: re-correr la misma
              fecha pisa la misma carpeta.
        """
        # logical_date es None si la corrida se disparó por API sin fecha → run_after
        ingestion_date = logical_date or dag_run.run_after

        src = SOURCE_DIR / SOURCE_FILES[table]
        if not src.is_file():
            raise FileNotFoundError(f"No existe el archivo fuente de '{table}': {src}")

        dest_dir = (
            BRONZE_DIR
            / table
            / f"ingested_year={ingestion_date:%Y}"
            / f"ingested_month={ingestion_date:%m}"
        )
        if dest_dir.exists():
            shutil.rmtree(dest_dir)
        dest_dir.mkdir(parents=True)

        dest = dest_dir / src.name
        shutil.copy2(src, dest)
        # no usamos pandas porque reescribe el archivo 
        # y cambia el tamaño de bytes (aunque los datos sean iguales)

        src_bytes, dest_bytes = src.stat().st_size, dest.stat().st_size
        if dest_bytes != src_bytes:
            raise OSError(f"Copia incompleta de '{table}': {dest_bytes} de {src_bytes} bytes")

        print(f"[extract] {table}: {src} → {dest} ({dest_bytes / 1e6:.1f} MB)")
        return str(dest)

    @task(retries=0)  # si los datos están mal, reintentar no los arregla
    def validate_bronze(paths: list[str]) -> None:
        """Compuerta antes de silver: las 7 tablas aterrizaron bien.

        Pre:  corrieron todas las instancias de extract; `paths` son sus rutas (vía XCom).
        Post: falla el DAG si falta una tabla, si un archivo no existe, está vacío
              o no tiene al menos encabezado + una fila de datos.
        """
        paths = [Path(p) for p in paths]
        errors = []

        # bronze/{table}/ingested_year=YYYY/ingested_month=MM/{archivo} → parents[2] es {table}
        missing = set(SOURCE_FILES) - {p.parents[2].name for p in paths}
        if missing:
            errors.append(f"faltan tablas: {sorted(missing)}")

        for path in paths:
            if not path.is_file():
                errors.append(f"no existe: {path}")
                continue

            # Solo las 2 primeras líneas: no hace falta leer los 394 MB de events
            with path.open(encoding="utf-8") as f:
                header, first_row = f.readline(), f.readline()

            if not header.strip():
                errors.append(f"vacío: {path}")
            elif not first_row.strip():
                errors.append(f"sin filas de datos: {path}")
            else:
                print(f"[validate_bronze] OK {path.parents[2].name}: "
                      f"{path.stat().st_size / 1e6:.1f} MB | {header.strip()}")

        # Se juntan todos los errores y se falla una sola vez, mostrando todo lo que está mal
        if errors:
            raise ValueError("Bronze inválido:\n  - " + "\n  - ".join(errors))

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

    # Pasar la salida de extract como argumento ya crea la dependencia extract >> validate_bronze
    bronze_paths = extract.expand(table=list(SOURCE_FILES))

    (
        validate_bronze(bronze_paths)
        >> silver()
        >> validate_silver()
        >> build_gold()
        >> validate_gold()
        >> [register_duckdb(), finops_report()]
    )


medallion_pipeline_dag()