-- Catálogo de gold en DuckDB: las consultas que ejecuta la tarea register_duckdb
-- de dags/medallion_pipeline_dag.py, en el mismo orden.
--
-- En el pipeline la vista se crea con la ruta del contenedor escrita literal
-- ('/opt/airflow/data/gold/order_items_enriched/**/*.parquet') y se guarda en
-- data/catalog.duckdb. Acá la ruta sale de una variable para poder correr el
-- archivo desde Windows contra los mismos Parquet.
--
-- Cómo correrlo (desde la raíz del repo, con `pip install duckdb-cli`):
--     duckdb -f sql/gold_queries.sql
-- Antes, cambiá la ruta de la línea SET VARIABLE por la de tu carpeta del docker-compose.

SET VARIABLE gold_dir = 'C:/Users/User/airflow-docker/data/gold/order_items_enriched';

-- 1) Vista sobre todas las particiones. No copia datos: guarda la consulta, así que
--    una partición nueva aparece sola sin volver a registrarla. hive_partitioning
--    convierte las carpetas year=/month= en columnas, y hive_types las fija como
--    texto para que month = '05' compare exacto (sin eso, '05' pasaría a ser 5).
CREATE OR REPLACE VIEW gold_order_items_enriched AS
SELECT *
FROM read_parquet(
    getvariable('gold_dir') || '/**/*.parquet',
    hive_partitioning = true,
    hive_types = {'year': 'VARCHAR', 'month': 'VARCHAR'}
);

-- 2) Particiones registradas y filas de cada una (lo que loguea la tarea).
SELECT year, month, COUNT(*) AS filas
FROM gold_order_items_enriched
GROUP BY year, month
ORDER BY year, month;

-- 3) Totales del catálogo: filas, órdenes distintas e ingresos.
SELECT
    COUNT(*)                   AS filas,
    COUNT(DISTINCT order_id)   AS ordenes,
    ROUND(SUM(sale_price), 2)  AS ingresos
FROM gold_order_items_enriched;

-- 4) Consulta de ejemplo que la tarea imprime al terminar: una sola partición.
SELECT *
FROM gold_order_items_enriched
WHERE year = '2023' AND month = '05'
LIMIT 10;
