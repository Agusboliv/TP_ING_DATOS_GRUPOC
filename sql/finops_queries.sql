-- Validación FinOps: las consultas que ejecutan la tarea finops_report de
-- dags/medallion_pipeline_dag.py y scripts/finops_validation.py.
--
-- Se corre la MISMA agregación sobre gold dos veces, sin y con filtro de partición,
-- y se compara qué leyó el motor. Lo único que cambia entre las dos es el WHERE.
-- La consulta filtrada tiene que abrir menos archivos Parquet: si abre los mismos,
-- el particionado no funciona.
--
-- Cómo correrlo (desde la raíz del repo, con `pip install duckdb-cli`):
--     duckdb -f sql/finops_queries.sql
-- Antes, cambiá la ruta de la línea SET VARIABLE por la de tu carpeta del docker-compose,
-- y si querés medir otro mes, cambiá '2023' y '05' en las consultas filtradas.

SET VARIABLE gold_dir = 'C:/Users/User/airflow-docker/data/gold/order_items_enriched';

-- Fuente común a todas las consultas: las particiones de gold como columnas de texto.
CREATE OR REPLACE TEMP VIEW gold AS
SELECT *
FROM read_parquet(
    getvariable('gold_dir') || '/**/*.parquet',
    hive_partitioning = true,
    hive_types = {'year': 'VARCHAR', 'month': 'VARCHAR'}
);

-- Igual que gold, pero con la columna filename: cada fila sabe de qué Parquet salió.
CREATE OR REPLACE TEMP VIEW gold_with_file AS
SELECT *
FROM read_parquet(
    getvariable('gold_dir') || '/**/*.parquet',
    hive_partitioning = true,
    hive_types = {'year': 'VARCHAR', 'month': 'VARCHAR'},
    filename = true
);

-- 1) Particiones disponibles (la tarea elige de acá el mes a medir).
SELECT DISTINCT year, month
FROM gold
ORDER BY year, month;

-- 2) FULL SCAN: la agregación sin filtro de partición.
EXPLAIN ANALYZE
SELECT year, month, COUNT(*) AS registros, ROUND(SUM(sale_price), 2) AS ingresos
FROM gold
GROUP BY year, month
ORDER BY year, month;

-- 3) PRUNED: la misma agregación con filtro sobre las columnas de partición.
--    En el plan, el nodo de lectura muestra "File Filters" y "Scanning Files: 1/N".
EXPLAIN ANALYZE
SELECT year, month, COUNT(*) AS registros, ROUND(SUM(sale_price), 2) AS ingresos
FROM gold
WHERE year = '2023' AND month = '05'
GROUP BY year, month
ORDER BY year, month;

-- 4) Archivos y filas leídos en cada caso: la tabla de resumen del reporte.
SELECT
    'FULL SCAN'                              AS escenario,
    (SELECT COUNT(DISTINCT filename) FROM gold_with_file) AS archivos,
    (SELECT COUNT(*) FROM gold)              AS filas
UNION ALL
SELECT
    'PRUNED',
    (SELECT COUNT(DISTINCT filename) FROM gold_with_file WHERE year = '2023' AND month = '05'),
    (SELECT COUNT(*) FROM gold WHERE year = '2023' AND month = '05');
