from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
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


# ── Helpers compartidos por las tareas ─────────────────────────────────────────

def bronze_dir(table: str, ingestion_date: datetime) -> Path:
    """Carpeta de bronze de `table` para una fecha de ingesta (particionado estilo Hive)."""
    return (
        BRONZE_DIR
        / table
        / f"ingested_year={ingestion_date:%Y}"
        / f"ingested_month={ingestion_date:%m}"
    )


def _write_parquet_dir(df: pd.DataFrame, out_dir: Path, table: str) -> Path:
    """Sobrescribe out_dir con un único Parquet (overwrite, nunca append)."""
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True)

    out = out_dir / f"{table}.parquet"
    df.to_parquet(out, index=False)
    return out


def write_silver(df: pd.DataFrame, table: str) -> Path:
    """Sobrescribe SILVER_DIR/{table}/ con un único Parquet (overwrite, nunca append)."""
    return _write_parquet_dir(df, SILVER_DIR / table, table)


def write_staging(df: pd.DataFrame, table: str) -> Path:
    """Igual que write_silver pero en STAGING_DIR: datos con PII que no pueden llegar a silver."""
    return _write_parquet_dir(df, STAGING_DIR / table, table)


@dag(
    dag_id="medallion_pipeline_dag",
    # @monthly porque la partición de gold es mensual: con @daily, las 30 corridas
    # de un mes reescriben la misma carpeta. start_date arranca con el dataset
    # (2019-01); no se pone end_date para que un trigger manual con la fecha de hoy
    # pueda correr el pipeline completo. Los meses sin ventas no escriben partición.
    schedule="@monthly",
    start_date=datetime(2019, 1, 1),
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

        dest_dir = bronze_dir(table, ingestion_date)
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
    def clean_orders(logical_date: datetime | None = None, dag_run=None) -> str:
        """Tipa, normaliza y deduplica orders de bronze y lo escribe en silver.

        Pre:  bronze/orders de la fecha de ingesta de esta corrida existe (validate_bronze pasó).
        Post: SILVER_DIR/orders/orders.parquet con tipos explícitos; order_id único.
        Inv:  los nulos de shipped/delivered/returned_at se conservan como NaT: no son datos
              faltantes, son eventos que todavía no ocurrieron según el status.
        """
        src = bronze_dir("orders", logical_date or dag_run.run_after) / SOURCE_FILES["orders"]

        # Todo como texto: los tipos los decidimos nosotros, no la inferencia de pandas
        df = pd.read_csv(src, dtype=str)
        rows_in = len(df)

        # Nulos que invalidan la fila entera. Se descartan ANTES de deduplicar para
        # que un order_id nulo no compita como clave:
        #   order_id   sin PK la orden no se puede unir a sus ítems
        #   user_id    sin cliente no se puede unir a users ni atribuir la venta
        #   created_at es la fecha de negocio que particiona gold: sin ella no hay partición
        #   status     gobierna la regla status ↔ fechas de más abajo
        # gender y num_of_item NO entran: una orden sin género declarado o sin
        # cantidad sigue siendo una venta válida y contable.
        required = ["order_id", "user_id", "created_at", "status"]
        df = df.dropna(subset=required)
        rows_null = rows_in - len(df)

        # Dedup por clave primaria (antes de tipar, sobre el valor crudo)
        df = df.drop_duplicates(subset="order_id")

        # Tipado de ids y cantidades
        df["order_id"] = df["order_id"].astype("int64")
        df["user_id"] = df["user_id"].astype("int64")
        df["num_of_item"] = df["num_of_item"].astype("int8")  # valores 1 a 4

        # Normalización de categóricas (en el CSV ya vienen limpias; esto blinda re-cargas)
        df["status"] = df["status"].str.strip().str.title()
        df["gender"] = df["gender"].str.strip().str.upper()

        # Fechas: ISO8601 acepta las dos variantes del CSV (con y sin microsegundos).
        # Las celdas vacías quedan como NaT, que en Parquet se guarda como NULL de tipo timestamp.
        # as_unit("us"): el CSV no tiene más precisión que microsegundos, y así DuckDB lo lee
        # como TIMESTAMPTZ estándar en vez de TIMESTAMPTZ_NS.
        date_cols = ["created_at", "shipped_at", "delivered_at", "returned_at"]
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], format="ISO8601", utc=True).dt.as_unit("us")

        # Coherencia status ↔ fechas: qué fechas tiene que tener cada status (el resto, nulas)
        expected_dates = {
            "Processing": set(),
            "Cancelled": set(),
            "Shipped": {"shipped_at"},
            "Complete": {"shipped_at", "delivered_at"},
            "Returned": {"shipped_at", "delivered_at", "returned_at"},
        }
        unknown = set(df["status"]) - set(expected_dates)
        if unknown:
            raise ValueError(f"status desconocidos en orders: {sorted(unknown)}")

        incoherent = pd.Series(False, index=df.index)
        for col in ["shipped_at", "delivered_at", "returned_at"]:
            should_have = df["status"].map(lambda s: col in expected_dates[s])
            incoherent |= df[col].notna() != should_have

        out = write_silver(df, "orders")
        print(f"[clean_orders] {rows_in} filas leídas | {rows_null} con nulos en claves "
              f"| {rows_in - rows_null - len(df)} duplicadas eliminadas "
              f"| {incoherent.sum()} incoherentes status/fechas | {len(df)} escritas en {out}")
        print(f"[clean_orders] nulos: {df[date_cols].isna().sum().to_dict()}")
        return str(out)

    @task
    def clean_order_items(logical_date: datetime | None = None, dag_run=None) -> str:
        """Tipa, normaliza y deduplica order_items de bronze y lo escribe en silver.

        Pre:  bronze/order_items de la fecha de ingesta de esta corrida existe (validate_bronze pasó).
        Post: SILVER_DIR/order_items/order_items.parquet con tipos explícitos; id único.
        Inv:  los nulos de shipped/delivered/returned_at se conservan como NaT: el ítem avanza
              junto con su orden, así que la fecha ausente es un evento que no ocurrió.
        """
        src = bronze_dir("order_items", logical_date or dag_run.run_after) / SOURCE_FILES["order_items"]

        # Todo como texto: los tipos los decidimos nosotros, no la inferencia de pandas
        df = pd.read_csv(src, dtype=str)
        rows_in = len(df)

        # Nulos que invalidan la fila entera, descartados antes de deduplicar.
        # Acá entran las cinco claves: sin ellas el ítem no se puede atribuir a
        # ninguna orden, cliente, producto ni unidad de inventario, y el grano de
        # gold deja de tener sentido. created_at y status gobiernan la partición y
        # la regla de coherencia. sale_price no entra en esta lista: se valida
        # aparte y hace fallar la tarea, porque un importe perdido altera las sumas.
        required = ["id", "order_id", "user_id", "product_id", "inventory_item_id",
                    "created_at", "status"]
        df = df.dropna(subset=required)
        rows_null = rows_in - len(df)

        # Dedup por clave primaria (antes de tipar, sobre el valor crudo)
        df = df.drop_duplicates(subset="id")

        # Tipado de la PK y de las 4 claves foráneas. int64 igual que en clean_orders:
        # en gold se unen order_id ↔ orders.order_id y user_id ↔ users.id, y un join
        # entre tipos distintos es una fuente silenciosa de filas perdidas.
        for col in ["id", "order_id", "user_id", "product_id", "inventory_item_id"]:
            df[col] = df[col].astype("int64")

        # sale_price es el único valor monetario de la tabla y el que gold agrega
        # (ingresos y margen), así que va a float64 y se valida el rango.
        df["sale_price"] = df["sale_price"].astype("float64")
        invalid_price = df["sale_price"].isna() | (df["sale_price"] < 0)
        if invalid_price.any():
            raise ValueError(f"{invalid_price.sum()} filas con sale_price nulo o negativo")

        # Normalización de categóricas (en el CSV ya vienen limpias; esto blinda re-cargas)
        df["status"] = df["status"].str.strip().str.title()

        # Mismo criterio de fechas que orders: ISO8601 para las dos variantes del CSV
        # y microsegundos para que DuckDB lo lea como TIMESTAMPTZ estándar.
        date_cols = ["created_at", "shipped_at", "delivered_at", "returned_at"]
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], format="ISO8601", utc=True).dt.as_unit("us")

        # El ítem hereda el ciclo de vida de su orden, así que vale la misma regla
        # status ↔ fechas que en clean_orders.
        expected_dates = {
            "Processing": set(),
            "Cancelled": set(),
            "Shipped": {"shipped_at"},
            "Complete": {"shipped_at", "delivered_at"},
            "Returned": {"shipped_at", "delivered_at", "returned_at"},
        }
        unknown = set(df["status"]) - set(expected_dates)
        if unknown:
            raise ValueError(f"status desconocidos en order_items: {sorted(unknown)}")

        incoherent = pd.Series(False, index=df.index)
        for col in ["shipped_at", "delivered_at", "returned_at"]:
            should_have = df["status"].map(lambda s: col in expected_dates[s])
            incoherent |= df[col].notna() != should_have

        out = write_silver(df, "order_items")
        print(f"[clean_order_items] {rows_in} filas leídas | {rows_null} con nulos en claves "
              f"| {rows_in - rows_null - len(df)} duplicadas eliminadas "
              f"| {incoherent.sum()} incoherentes status/fechas | {len(df)} escritas en {out}")
        print(f"[clean_order_items] nulos: {df[date_cols].isna().sum().to_dict()}")
        return str(out)

    @task
    def clean_inventory_items(logical_date: datetime | None = None, dag_run=None) -> str:
        """Tipado de cost y product_retail_price; sold_at nulo = ítem aún en stock; dedup por id.

        Pre:  bronze/inventory_items de la fecha de ingesta de esta corrida existe.
        Post: SILVER_DIR/inventory_items/inventory_items.parquet; id único.
        Inv:  sold_at nulo se conserva: es stock todavía no vendido, no un dato faltante.
        """
        src = bronze_dir("inventory_items", logical_date or dag_run.run_after) / SOURCE_FILES["inventory_items"]

        df = pd.read_csv(src, dtype=str)
        rows_in = len(df)

        # Nulos que invalidan la unidad de inventario: sin id no hay PK, sin
        # product_id no se sabe qué unidad es, sin created_at no se sabe cuándo
        # entró al stock y sin cost no se puede calcular margen en gold.
        # sold_at NO entra: su ausencia es justamente la señal de "sin vender".
        required = ["id", "product_id", "created_at", "cost", "product_distribution_center_id"]
        df = df.dropna(subset=required)
        rows_null = rows_in - len(df)

        df = df.drop_duplicates(subset="id")

        for col in ["id", "product_id", "product_distribution_center_id"]:
            df[col] = df[col].astype("int64")

        df["cost"] = df["cost"].astype("float64")
        df["product_retail_price"] = df["product_retail_price"].astype("float64")
        if (df["cost"] < 0).any():
            raise ValueError(f"{int((df['cost'] < 0).sum())} unidades con cost negativo")

        for col in ["created_at", "sold_at"]:
            df[col] = pd.to_datetime(df[col], format="ISO8601", utc=True).dt.as_unit("us")

        # Una unidad no se puede vender antes de entrar al stock. Se cuenta y se
        # loguea sin eliminar: es una anomalía a investigar, no una fila inútil.
        impossible = (df["sold_at"].notna() & (df["sold_at"] < df["created_at"])).sum()

        # Atributos del producto copiados en esta tabla (ver README: coinciden al
        # 100% con products). Se normalizan igual para que silver sea consistente.
        df["product_category"] = df["product_category"].str.strip().str.title()
        df["product_department"] = df["product_department"].str.strip().str.title()
        df["product_brand"] = df["product_brand"].str.strip().str.title()
        df["product_name"] = df["product_name"].str.strip()
        df["product_sku"] = df["product_sku"].str.strip().str.upper()

        out = write_silver(df, "inventory_items")
        print(f"[clean_inventory_items] {rows_in} filas leídas | {rows_null} con nulos en claves "
              f"| {rows_in - rows_null - len(df)} duplicadas eliminadas "
              f"| {impossible} vendidas antes de existir | {len(df)} escritas en {out}")
        print(f"[clean_inventory_items] sin vender (sold_at nulo): {int(df['sold_at'].isna().sum())}")
        return str(out)

    @task
    def clean_products(logical_date: datetime | None = None, dag_run=None) -> str:
        """Tipado de cost/retail_price; casing de category/department; regla para name/brand nulos.

        Pre:  bronze/products de la fecha de ingesta de esta corrida existe.
        Post: SILVER_DIR/products/products.parquet; id único.
        Inv:  name y brand nulos se conservan: un producto sin nombre comercial igual
              se vende, tiene categoría y tiene precio.
        """
        src = bronze_dir("products", logical_date or dag_run.run_after) / SOURCE_FILES["products"]

        df = pd.read_csv(src, dtype=str)
        rows_in = len(df)

        # Sin id no hay PK; sin cost ni retail_price no hay margen en gold; sin
        # category/department el producto no se puede agrupar en ningún análisis;
        # sin distribution_center_id se pierde la trazabilidad logística.
        # name y brand quedan afuera a propósito (ver Inv).
        required = ["id", "cost", "retail_price", "category", "department", "distribution_center_id"]
        df = df.dropna(subset=required)
        rows_null = rows_in - len(df)

        df = df.drop_duplicates(subset="id")

        df["id"] = df["id"].astype("int64")
        df["distribution_center_id"] = df["distribution_center_id"].astype("int64")
        df["cost"] = df["cost"].astype("float64")
        df["retail_price"] = df["retail_price"].astype("float64")

        invalid = (df["cost"] < 0) | (df["retail_price"] < 0)
        if invalid.any():
            raise ValueError(f"{int(invalid.sum())} productos con precio o costo negativo")

        # Productos que se venden por debajo del costo: no es un error de datos,
        # pero se loguea porque explica márgenes negativos en gold.
        below_cost = int((df["retail_price"] < df["cost"]).sum())

        df["category"] = df["category"].str.strip().str.title()
        df["department"] = df["department"].str.strip().str.title()
        df["brand"] = df["brand"].str.strip().str.title()
        df["name"] = df["name"].str.strip()
        df["sku"] = df["sku"].str.strip().str.upper()

        out = write_silver(df, "products")
        print(f"[clean_products] {rows_in} filas leídas | {rows_null} con nulos en claves "
              f"| {rows_in - rows_null - len(df)} duplicadas eliminadas "
              f"| {below_cost} con precio bajo el costo | {len(df)} escritas en {out}")
        print(f"[clean_products] nulos conservados: name={int(df['name'].isna().sum())}, "
              f"brand={int(df['brand'].isna().sum())}")
        return str(out)

    @task
    def clean_distribution_centers(logical_date: datetime | None = None, dag_run=None) -> str:
        """Tipado de latitude/longitude; dedup por id.

        Pre:  bronze/distribution_centers de la fecha de ingesta de esta corrida existe.
        Post: SILVER_DIR/distribution_centers/distribution_centers.parquet; id único.
        Inv:  `name` solo se le quitan espacios: lleva la sigla del estado ("Memphis TN")
              y pasarlo a formato título la rompería ("Memphis Tn").
        """
        src = bronze_dir("distribution_centers", logical_date or dag_run.run_after) / SOURCE_FILES["distribution_centers"]

        df = pd.read_csv(src, dtype=str)
        rows_in = len(df)

        # Las cuatro columnas son obligatorias: es una dimensión de 10 filas donde
        # cada una se usa entera (el id para unir, el nombre para mostrar y las
        # coordenadas para cualquier análisis geográfico).
        required = ["id", "name", "latitude", "longitude"]
        df = df.dropna(subset=required)
        rows_null = rows_in - len(df)

        df = df.drop_duplicates(subset="id")

        df["id"] = df["id"].astype("int64")
        df["latitude"] = df["latitude"].astype("float64")
        df["longitude"] = df["longitude"].astype("float64")
        df["name"] = df["name"].str.strip()

        # Coordenadas fuera de rango en una tabla de 10 filas es un error de carga,
        # no un outlier: se corta la corrida.
        out_of_range = (
            df["latitude"].abs().gt(90) | df["longitude"].abs().gt(180)
        )
        if out_of_range.any():
            raise ValueError(f"{int(out_of_range.sum())} centros con coordenadas fuera de rango")

        out = write_silver(df, "distribution_centers")
        print(f"[clean_distribution_centers] {rows_in} filas leídas | {rows_null} con nulos en claves "
              f"| {rows_in - rows_null - len(df)} duplicadas eliminadas | {len(df)} escritas en {out}")
        return str(out)


    @task
    def clean_users(logical_date: datetime | None = None, dag_run=None) -> str:
        """Tipado de age/created_at; casing de city/state/country; dedup por id. Todavía con PII.

        Pre:  bronze/users de la fecha de ingesta de esta corrida existe.
        Post: STAGING_DIR/users/users.parquet (fuera de silver, porque todavía tiene PII
              en claro). Lo consume mask_pii, que es quien escribe en silver.
        Inv:  no se deduplica por email: 15.989 usuarios comparten dirección con otro
              (ver README). La identidad de negocio es `id`, no el mail.
        """
        src = bronze_dir("users", logical_date or dag_run.run_after) / SOURCE_FILES["users"]

        df = pd.read_csv(src, dtype=str)
        rows_in = len(df)

        # Sin id no hay PK; sin email no hay hash con el que unir en gold; sin
        # created_at no se sabe desde cuándo es cliente. city/state y las
        # coordenadas quedan afuera: un domicilio incompleto no invalida al cliente.
        required = ["id", "email", "created_at"]
        df = df.dropna(subset=required)
        rows_null = rows_in - len(df)

        df = df.drop_duplicates(subset="id")

        df["id"] = df["id"].astype("int64")
        df["age"] = df["age"].astype("int8")  # rango real 12 a 70
        df["latitude"] = df["latitude"].astype("float64")
        df["longitude"] = df["longitude"].astype("float64")
        df["created_at"] = pd.to_datetime(df["created_at"], format="ISO8601", utc=True).dt.as_unit("us")

        # El email se normaliza ACÁ y no en mask_pii: el hash se calcula sobre el
        # texto final, así que "Juan@Mail.com" y "juan@mail.com" tienen que llegar
        # iguales o producirían dos hashes distintos para la misma persona.
        df["email"] = df["email"].str.strip().str.lower()

        df["gender"] = df["gender"].str.strip().str.upper()
        for col in ["state", "city", "country", "traffic_source"]:
            df[col] = df[col].str.strip().str.title()

        # Nombres y dirección: solo se les quitan espacios. Son PII que mask_pii
        # va a descartar, y aplicarles formato título alteraría apellidos legítimos.
        for col in ["first_name", "last_name", "street_address"]:
            df[col] = df[col].str.strip()

        out = write_staging(df, "users")
        print(f"[clean_users] {rows_in} filas leídas | {rows_null} con nulos en claves "
              f"| {rows_in - rows_null - len(df)} duplicadas eliminadas | {len(df)} escritas en {out}")
        print(f"[clean_users] emails repetidos entre usuarios: {int(df['email'].duplicated().sum())} "
              f"| city nula: {int(df['city'].isna().sum())}")
        return str(out)

    @task
    def mask_pii() -> str:
        """Hashea email con SHA-256 + salt; descarta los identificadores directos.

        Pre:  Variable PII_SALT definida; se lee acá adentro, nunca a nivel módulo.
              STAGING_DIR/users/ escrito por clean_users.
        Post: SILVER_DIR/users/ sin PII en claro; STAGING_DIR/users/ borrado.
        Inv:  mismo email + mismo salt → mismo hash (joinable, no reversible sin el salt).
        """
        # Variable.get() DENTRO de la task: a nivel módulo se ejecutaría en cada
        # parseo del DAG (cada ~30 s) y golpearía la metadata DB al pedo.
        try:
            salt = Variable.get("PII_SALT")
        except Exception as exc:
            raise ValueError(
                "Falta la Airflow Variable 'PII_SALT'. Cargala con "
                "`airflow variables set PII_SALT <valor>` o desde Admin → Variables. "
                "No se hashea sin salt: un SHA-256 pelado de emails se revierte por diccionario."
            ) from exc

        src = STAGING_DIR / "users" / "users.parquet"
        df = pd.read_parquet(src)
        rows_in = len(df)

        # clean_users garantiza email no nulo, pero si algo cambiara aguas arriba
        # un nulo acá sería grave: f"{salt}{None}" hashea el texto "None" y les
        # daría a todos los nulos el mismo hash, como si fueran el mismo cliente.
        if df["email"].isna().any():
            raise ValueError(f"{int(df['email'].isna().sum())} emails nulos llegaron a mask_pii")

        # El email ya viene en minúsculas y sin espacios desde clean_users: el hash
        # se calcula sobre ese texto final para que sea estable entre corridas.
        df["email_hash"] = [
            hashlib.sha256(f"{salt}{email}".encode()).hexdigest() for email in df["email"]
        ]

        # Identificadores directos: se borran, no se hashean. Un hash solo sirve si
        # necesitás unir por esa columna, y nadie agrupa ventas por nombre o calle.
        # postal_code y lat/long también salen: 2.515 usuarios son los únicos con su
        # código postal, así que ubican a una persona igual que la dirección.
        direct_identifiers = [
            "first_name", "last_name", "email", "street_address",
            "postal_code", "latitude", "longitude",
        ]
        df = df.drop(columns=direct_identifiers)

        # Red de seguridad: que no quede ninguna columna de PII por un cambio futuro
        # en el esquema de origen.
        leaked = sorted(set(direct_identifiers) & set(df.columns))
        if leaked:
            raise ValueError(f"PII sin enmascarar en silver: {leaked}")

        out = write_silver(df, "users")

        # El staging existe solo para este paso: una vez publicado silver, la copia
        # con PII en claro no tiene por qué seguir en disco.
        shutil.rmtree(STAGING_DIR / "users")

        print(f"[mask_pii] {rows_in} filas | {df['email_hash'].nunique()} hashes distintos "
              f"| columnas publicadas: {list(df.columns)}")
        print(f"[mask_pii] staging borrado | nulos en silver: "
              f"{ {c: int(n) for c, n in df.isna().sum().items() if n} }")
        return str(out)

    @task_group(group_id="silver")
    def silver():
        clean_orders()
        clean_order_items()
        clean_inventory_items()
        clean_products()
        clean_distribution_centers()
        clean_users() >> mask_pii()

    @task(retries=0)  # si los datos están mal, reintentar no los arregla
    def validate_silver() -> None:
        """Compuerta antes de gold: unicidad de PKs e integridad referencial entre tablas.

        Pre:  todas las tablas silver escritas.
        Post: loggea huérfanos de order_items → orders/products/users/inventory_items
              y de products → distribution_centers; falla si hay PKs duplicadas.
        """
        primary_keys = {
            "orders": "order_id",
            "order_items": "id",
            "products": "id",
            "inventory_items": "id",
            "distribution_centers": "id",
            "users": "id",
        }

        # (tabla hija, FK) → (tabla padre, PK). Son los joins que arma build_gold:
        # un huérfano acá es una fila que el INNER JOIN de gold va a descartar.
        foreign_keys = [
            ("order_items", "order_id", "orders", "order_id"),
            ("order_items", "user_id", "users", "id"),
            ("order_items", "product_id", "products", "id"),
            ("order_items", "inventory_item_id", "inventory_items", "id"),
            ("products", "distribution_center_id", "distribution_centers", "id"),
        ]

        files = {table: SILVER_DIR / table / f"{table}.parquet" for table in primary_keys}
        missing = sorted(table for table, path in files.items() if not path.is_file())
        if missing:
            raise ValueError(f"faltan tablas en silver: {missing}")

        # Se leen solo las columnas de claves: validar integridad no necesita
        # cargar los 490.705 registros completos de inventory_items.
        needed = {table: {pk} for table, pk in primary_keys.items()}
        for child, fk, parent, pk in foreign_keys:
            needed[child].add(fk)
            needed[parent].add(pk)
        data = {t: pd.read_parquet(p, columns=sorted(needed[t])) for t, p in files.items()}

        errors: list[str] = []

        # 1) Unicidad de PK: esto SÍ corta la corrida. Una PK duplicada multiplica
        # filas en el join de gold y rompe el grano del modelo en silencio.
        for table, pk in primary_keys.items():
            duplicated = int(data[table][pk].duplicated().sum())
            if duplicated:
                errors.append(f"{table}: {duplicated} valores repetidos en la PK '{pk}'")
            print(f"[validate_silver] {table}: {len(data[table])} filas | PK '{pk}' "
                  f"{'DUPLICADA' if duplicated else 'única'}")

        # 2) Integridad referencial: se loguea, no se corta. Un huérfano es un dato
        # a investigar, pero la decisión de qué hacer con él es de build_gold.
        for child, fk, parent, pk in foreign_keys:
            valid = set(data[parent][pk])
            orphans = int((~data[child][fk].isin(valid)).sum())
            total = len(data[child])
            print(f"[validate_silver] {child}.{fk} → {parent}.{pk}: {orphans} huérfanos "
                  f"de {total} ({orphans / total:.2%})")

        if errors:
            raise ValueError("Silver no pasó la validación:\n  - " + "\n  - ".join(errors))

        print(f"[validate_silver] {len(primary_keys)} tablas con PK única y "
              f"{len(foreign_keys)} relaciones verificadas")

    # ── Gold ──────────────────────────────────────────────────────────────────

    @task(outlets=[GOLD_ASSET])
    def build_gold(logical_date: datetime | None = None, dag_run=None) -> str:
        """Join order_items + orders + products + users (hasheado) + distribution_centers.

        Pre:  silver validado; ventana = mes calendario de logical_date sobre orders.created_at.
        Post: GOLD_DIR/{GOLD_DATASET}/year=YYYY/month=MM/ solo con ítems de órdenes de ese mes.
        Inv:  se sobreescribe únicamente esa partición; grano = un ítem de orden.
        """
        # Ventana = mes calendario de la fecha lógica. Derivarla así (y no de
        # data_interval_end) hace que un trigger manual, un backfill y una corrida
        # programada procesen exactamente el mismo rango.
        run_date = pd.Timestamp(logical_date or dag_run.run_after)
        run_date = run_date.tz_localize("UTC") if run_date.tzinfo is None else run_date.tz_convert("UTC")
        window_start = run_date.normalize().replace(day=1)
        window_end = window_start + pd.offsets.MonthBegin(1)

        orders = pd.read_parquet(SILVER_DIR / "orders" / "orders.parquet")
        month_orders = orders[
            (orders["created_at"] >= window_start) & (orders["created_at"] < window_end)
        ]

        out_dir = GOLD_DIR / GOLD_DATASET / f"year={window_start:%Y}" / f"month={window_start:%m}"

        if month_orders.empty:
            # Mes sin ventas: no se publica una partición vacía. Si quedó una de
            # una corrida anterior se borra, porque la partición tiene que reflejar
            # lo que hay en silver. Pasa cuando la fecha lógica cae fuera del rango
            # del dataset (2019-01 a 2024-01), por ejemplo en un trigger manual de hoy.
            if out_dir.exists():
                shutil.rmtree(out_dir)
            print(f"[build_gold] {window_start:%Y-%m}: 0 órdenes en silver, no se escribe "
                  f"partición. El dataset va de 2019-01 a 2024-01.")
            return ""

        # El ítem pertenece al mes de SU ORDEN, no al de su propio created_at
        # (que puede caer días después). Por eso se filtra por order_id y no por fecha.
        items = pd.read_parquet(SILVER_DIR / "order_items" / "order_items.parquet")
        items = items[items["order_id"].isin(set(month_orders["order_id"]))]

        users = pd.read_parquet(SILVER_DIR / "users" / "users.parquet")
        products = pd.read_parquet(SILVER_DIR / "products" / "products.parquet")
        centers = pd.read_parquet(SILVER_DIR / "distribution_centers" / "distribution_centers.parquet")

        # INNER contra orders: un ítem sin pedido no es una venta.
        # LEFT contra las dimensiones: un dato de catálogo o de logística faltante
        # no puede hacer desaparecer una venta real. Hoy validate_silver reporta
        # 0 huérfanos, así que ambas dan lo mismo; la diferencia protege a futuro.
        gold = (
            items.rename(columns={
                "id": "order_item_id",
                "status": "item_status",
                "shipped_at": "item_shipped_at",
                "delivered_at": "item_delivered_at",
                "returned_at": "item_returned_at",
            })
            .drop(columns=["created_at"])  # se usa la fecha de la orden, no la del ítem
            .merge(
                month_orders[["order_id", "status", "created_at", "num_of_item"]].rename(
                    columns={
                        "status": "order_status",
                        "created_at": "ordered_at",
                        "num_of_item": "items_in_order",
                    }
                ),
                on="order_id",
                how="inner",
            )
            .merge(
                users[["id", "email_hash", "age", "gender", "state", "city",
                       "country", "traffic_source"]].rename(columns={
                    "id": "user_id",
                    "email_hash": "user_email_hash",
                    "age": "user_age",
                    "gender": "user_gender",
                    "state": "user_state",
                    "city": "user_city",
                    "country": "user_country",
                    "traffic_source": "user_traffic_source",
                }),
                on="user_id",
                how="left",
            )
            .merge(
                products[["id", "name", "category", "brand", "department",
                          "cost", "retail_price", "distribution_center_id"]].rename(columns={
                    "id": "product_id",
                    "name": "product_name",
                    "category": "product_category",
                    "brand": "product_brand",
                    "department": "product_department",
                    "cost": "product_cost",
                    "retail_price": "product_retail_price",
                }),
                on="product_id",
                how="left",
            )
            .merge(
                centers[["id", "name"]].rename(columns={
                    "id": "distribution_center_id",
                    "name": "distribution_center",
                }),
                on="distribution_center_id",
                how="left",
            )
        )

        # Métricas derivadas: lo que el analista no debería tener que calcular.
        gold["margin"] = gold["sale_price"] - gold["product_cost"]
        gold["is_returned"] = gold["item_returned_at"].notna()
        gold["delivery_days"] = (
            gold["item_delivered_at"] - gold["ordered_at"]
        ).dt.total_seconds() / 86400

        columns = [
            "order_item_id", "order_id", "user_id", "product_id", "inventory_item_id",
            "ordered_at", "item_shipped_at", "item_delivered_at", "item_returned_at",
            "order_status", "item_status", "is_returned", "items_in_order", "delivery_days",
            "sale_price", "product_cost", "product_retail_price", "margin",
            "product_name", "product_category", "product_brand", "product_department",
            "distribution_center",
            "user_email_hash", "user_age", "user_gender",
            "user_state", "user_city", "user_country", "user_traffic_source",
        ]
        gold = gold[columns]

        # Red de seguridad: nada fuera de la ventana puede entrar a esta partición.
        outside = int((
            (gold["ordered_at"] < window_start) | (gold["ordered_at"] >= window_end)
        ).sum())
        if outside:
            raise ValueError(f"{outside} filas fuera de la ventana {window_start:%Y-%m}")

        # year y month NO van adentro del Parquet: viven en la ruta y DuckDB los
        # infiere con hive_partitioning. Escribirlos además sería duplicarlos.
        out = _write_parquet_dir(gold, out_dir, GOLD_DATASET)

        print(f"[build_gold] {window_start:%Y-%m}: {len(month_orders)} órdenes → {len(gold)} ítems "
              f"| {len(columns)} columnas | {out}")
        print(f"[build_gold] ingresos {gold['sale_price'].sum():,.2f} "
              f"| margen {gold['margin'].sum():,.2f} "
              f"| devueltos {int(gold['is_returned'].sum())}")
        return str(out)

    @task(retries=0)  # si la partición está mal, reintentar no la arregla
    def validate_gold(logical_date: datetime | None = None, dag_run=None) -> None:
        """Chequea grano único (order_items.id) y loggea conteo de la partición.

        Pre:  build_gold escribió la partición de esta corrida.
        Post: conteo loggeado → evidencia del test de idempotencia (dos corridas, mismo número).
        """
        run_date = pd.Timestamp(logical_date or dag_run.run_after)
        run_date = run_date.tz_localize("UTC") if run_date.tzinfo is None else run_date.tz_convert("UTC")
        window_start = run_date.normalize().replace(day=1)
        window_end = window_start + pd.offsets.MonthBegin(1)

        partition = (
            GOLD_DIR / GOLD_DATASET / f"year={window_start:%Y}" / f"month={window_start:%m}"
            / f"{GOLD_DATASET}.parquet"
        )
        if not partition.is_file():
            # Puede ser legítimo (mes sin ventas) o un fallo de build_gold. Se
            # distingue mirando silver: si hay órdenes de ese mes, falta la partición.
            orders = pd.read_parquet(SILVER_DIR / "orders" / "orders.parquet", columns=["created_at"])
            in_window = int((
                (orders["created_at"] >= window_start) & (orders["created_at"] < window_end)
            ).sum())
            if in_window:
                raise ValueError(
                    f"falta la partición {window_start:%Y-%m} y silver tiene "
                    f"{in_window} órdenes de ese mes"
                )
            print(f"[validate_gold] {window_start:%Y-%m}: 0 ventas en silver y sin partición. "
                  f"Correcto, nada que validar.")
            return

        df = pd.read_parquet(partition)
        errors: list[str] = []

        # 1) Grano: un registro por ítem vendido. Un order_item_id repetido
        # significa que algún join multiplicó filas y los ingresos salen inflados.
        duplicated = int(df["order_item_id"].duplicated().sum())
        if duplicated:
            errors.append(f"{duplicated} order_item_id repetidos: el join multiplicó filas")

        # 2) Ventana: la partición dice un mes, su contenido tiene que ser ese mes.
        outside = int((
            (df["ordered_at"] < window_start) | (df["ordered_at"] >= window_end)
        ).sum())
        if outside:
            errors.append(f"{outside} filas fuera de {window_start:%Y-%m}")

        # 3) Claves y métricas sin nulos: gold es la capa que consume el analista.
        for column in ["order_item_id", "order_id", "user_id", "ordered_at", "sale_price"]:
            nulls = int(df[column].isna().sum())
            if nulls:
                errors.append(f"{nulls} nulos en '{column}'")

        # 4) Gobernanza: ninguna columna de PII en claro puede haber llegado hasta acá.
        forbidden = {"email", "first_name", "last_name", "street_address",
                     "postal_code", "latitude", "longitude"}
        leaked = sorted(forbidden & set(df.columns))
        if leaked:
            errors.append(f"PII en claro en gold: {leaked}")

        if errors:
            raise ValueError(f"Gold {window_start:%Y-%m} no pasó la validación:\n  - "
                             + "\n  - ".join(errors))

        # Huella del contenido: dos corridas del mismo mes tienen que dar la misma.
        # Es más fuerte que comparar el conteo, porque detecta que las filas
        # cambiaron aunque sigan siendo la misma cantidad.
        fingerprint = hashlib.sha256(
            pd.util.hash_pandas_object(
                df.sort_values("order_item_id"), index=False
            ).values.tobytes()
        ).hexdigest()[:16]

        print(f"[validate_gold] {window_start:%Y-%m} OK | {len(df)} ítems "
              f"| {df['order_id'].nunique()} órdenes | {len(df.columns)} columnas")
        print(f"[validate_gold] ingresos {df['sale_price'].sum():,.2f} "
              f"| margen {df['margin'].sum():,.2f} "
              f"| devueltos {int(df['is_returned'].sum())}")
        print(f"[validate_gold] huella del contenido: {fingerprint} "
              f"(idéntica en cada corrida del mismo mes → idempotencia)")

    @task
    def register_duckdb() -> str:
        """Expone gold en DuckDB con CREATE OR REPLACE VIEW sobre read_parquet(hive_partitioning=true).

        Pre:  gold tiene al menos una partición escrita.
        Post: CATALOG_PATH con la vista actualizada.
        Inv:  la vista NO copia datos: guarda la consulta. Por eso el catálogo pesa
              unos KB y ve las particiones nuevas sin volver a registrarlas.
        """
        view = f"gold_{GOLD_DATASET}"
        pattern = f"{GOLD_DIR / GOLD_DATASET}/**/*.parquet"

        CATALOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        connection = duckdb.connect(str(CATALOG_PATH))
        try:
            # hive_types fuerza year/month a VARCHAR para que los filtros del
            # enunciado (year = '2023' AND month = '05') comparen exacto y no
            # dependan del autocast de DuckDB, que convertiría '05' en 5.
            connection.execute(
                f"""
                CREATE OR REPLACE VIEW {view} AS
                SELECT * FROM read_parquet(
                    '{pattern}',
                    hive_partitioning = true,
                    hive_types = {{'year': 'VARCHAR', 'month': 'VARCHAR'}}
                )
                """
            )

            partitions = connection.execute(
                f"SELECT year, month, COUNT(*) FROM {view} GROUP BY year, month ORDER BY year, month"
            ).fetchall()
            rows, orders, revenue = connection.execute(
                f"SELECT COUNT(*), COUNT(DISTINCT order_id), ROUND(SUM(sale_price), 2) FROM {view}"
            ).fetchone()
        finally:
            connection.close()

        years = sorted({year for year, _, _ in partitions})
        print(f"[register_duckdb] vista '{view}' creada en {CATALOG_PATH}")
        print(f"[register_duckdb] {len(partitions)} particiones ({years[0]}..{years[-1]}) "
              f"| {rows} filas | {orders} órdenes | ingresos {revenue:,.2f}")
        print(f"[register_duckdb] consultala con: "
              f"SELECT * FROM {view} WHERE year = '2023' AND month = '05'")
        return str(CATALOG_PATH)

    @task
    def finops_report(logical_date: datetime | None = None, dag_run=None) -> str:
        """Corre EXPLAIN ANALYZE full scan vs. pruned y guarda ambas salidas.

        Pre:  gold tiene al menos una partición.
        Post: reporte en DATA_DIR con archivos y filas leídas por cada query.
        Inv:  la consulta es la MISMA en los dos casos; lo único que cambia es el
              WHERE sobre las columnas de partición.
        """
        run_date = pd.Timestamp(logical_date or dag_run.run_after)
        run_date = run_date.tz_localize("UTC") if run_date.tzinfo is None else run_date.tz_convert("UTC")

        pattern = f"{GOLD_DIR / GOLD_DATASET}/**/*.parquet"
        source = (
            f"read_parquet('{pattern}', hive_partitioning = true, "
            "hive_types = {'year': 'VARCHAR', 'month': 'VARCHAR'})"
        )
        source_with_file = source.replace("hive_partitioning = true", "hive_partitioning = true, filename = true")

        connection = duckdb.connect()
        try:
            available = connection.execute(
                f"SELECT DISTINCT year, month FROM {source} ORDER BY year, month"
            ).fetchall()
            if not available:
                print("[finops_report] gold está vacío, no hay nada que medir")
                return ""

            # Se filtra el mes de la corrida; si ese mes no tiene partición
            # (fecha lógica fuera del dataset) se usa la última disponible, para
            # que la comparación siempre sea contra una partición real.
            target = (f"{run_date:%Y}", f"{run_date:%m}")
            if target not in available:
                target = available[-1]
                print(f"[finops_report] {run_date:%Y-%m} no tiene partición, "
                      f"se mide contra {target[0]}-{target[1]}")

            query = """
                SELECT year, month, COUNT(*) AS registros, ROUND(SUM(sale_price), 2) AS ingresos
                FROM {source}
                {where}
                GROUP BY year, month
                ORDER BY year, month
            """
            where = f"WHERE year = '{target[0]}' AND month = '{target[1]}'"

            results = {}
            for label, filter_clause in (("FULL SCAN", ""), ("PRUNED", where)):
                sql = query.format(source=source, where=filter_clause)

                start = time.perf_counter()
                connection.execute(sql).fetchall()
                elapsed_ms = (time.perf_counter() - start) * 1000

                plan = connection.execute("EXPLAIN ANALYZE " + sql).fetchall()[0][1]
                # Con filename=true cada fila sabe de qué Parquet salió: contar los
                # distintos prueba cuántos archivos abrió realmente el motor.
                files = connection.execute(
                    f"SELECT COUNT(DISTINCT filename) FROM {source_with_file} {filter_clause}"
                ).fetchone()[0]
                rows = connection.execute(
                    f"SELECT COUNT(*) FROM {source} {filter_clause}"
                ).fetchone()[0]

                results[label] = {"plan": plan, "files": files, "rows": rows, "ms": elapsed_ms}
        finally:
            connection.close()

        full, pruned = results["FULL SCAN"], results["PRUNED"]
        if pruned["files"] >= full["files"]:
            raise ValueError(
                f"el pruning no funciona: la consulta filtrada leyó {pruned['files']} "
                f"archivos y el scan completo {full['files']}. Revisar year=/month= en gold."
            )

        reduction = 100 * (1 - pruned["files"] / full["files"])
        report = DATA_DIR / "finops_report.txt"
        report.write_text(
            "\n".join([
                f"Dataset : {GOLD_DIR / GOLD_DATASET}",
                f"Filtro  : year = '{target[0]}' AND month = '{target[1]}'",
                f"Generado: {datetime.now(tz=run_date.tz).isoformat()}",
                "",
                "=== FULL SCAN (sin filtro de partición) ===",
                full["plan"],
                "",
                f"=== PRUNED SCAN (year='{target[0]}', month='{target[1]}') ===",
                pruned["plan"],
                "",
                f"{'':<26}{'FULL SCAN':>12}{'PRUNED':>12}",
                f"{'Archivos leídos':<26}{full['files']:>12}{pruned['files']:>12}",
                f"{'Filas leídas':<26}{full['rows']:>12}{pruned['rows']:>12}",
                f"{'Tiempo (ms)':<26}{full['ms']:>12.1f}{pruned['ms']:>12.1f}",
                "",
                f"El filtro de partición evitó abrir {full['files'] - pruned['files']} de "
                f"{full['files']} archivos ({reduction:.1f}% menos).",
            ]),
            encoding="utf-8",
        )

        print(f"[finops_report] FULL SCAN: {full['files']} archivos, {full['rows']} filas, "
              f"{full['ms']:.1f} ms")
        print(f"[finops_report] PRUNED   : {pruned['files']} archivos, {pruned['rows']} filas, "
              f"{pruned['ms']:.1f} ms")
        print(f"[finops_report] el pruning evitó {full['files'] - pruned['files']} de "
              f"{full['files']} archivos ({reduction:.1f}% menos) → {report}")
        return str(report)

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