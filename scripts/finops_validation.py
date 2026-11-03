"""Validación FinOps: prueba que el particionado de gold ahorra lectura.

Corre la misma consulta sobre gold con y sin filtro de partición (year/month)
usando EXPLAIN ANALYZE, y compara archivos, filas y datos leídos. Si la consulta
filtrada no abre menos archivos, termina con error. El reporte se imprime y se
guarda en docs/finops_report.txt.

Cómo correrlo en local (desde la raíz del repo, con `pip install duckdb`):
    python scripts/finops_validation.py --gold-dir "C:/Users/<usuario>/airflow-docker/data/gold/order_items_enriched"
    python scripts/finops_validation.py --gold-dir "..." --year 2023 --month 5
Sin --year/--month mide la última partición. `--help` lista el resto de opciones.

En el pipeline: la tarea finops_report de dags/medallion_pipeline_dag.py hace la
misma medición en cada corrida, después de validate_gold, y guarda su reporte en
data/finops_report.txt. Las consultas de los dos están en sql/finops_queries.sql.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import duckdb

GOLD_DATASET = "order_items_enriched"
DEFAULT_GOLD_DIR = (
    Path(os.environ.get("AIRFLOW_HOME", Path.home() / "airflow")) / "data" / "gold" / GOLD_DATASET
)
# El reporte va a docs/ del repo (scripts/ y docs/ son hermanas), y no a data/,
# que está en .gitignore: así la evidencia de FinOps queda versionada con la entrega.
# Se arma desde la ubicación del script, así funciona desde cualquier carpeta.
DEFAULT_REPORT = Path(__file__).resolve().parent.parent / "docs" / "finops_report.txt"

# La consulta es idéntica en los dos escenarios: lo único que cambia es el WHERE.
QUERY = """
SELECT year, month, COUNT(*) AS registros, ROUND(SUM(sale_price), 2) AS ingresos
FROM {source}
{where}
GROUP BY year, month
ORDER BY year, month
"""


def available_partitions(gold_dir: Path) -> list[tuple[str, str]]:
    """Particiones (year, month) que tienen al menos un Parquet, ordenadas.

    Se leen del disco y no con DuckDB: read_parquet sobre una carpeta sin archivos
    tira IOException en vez de devolver 0 filas.
    """
    partitions = {
        (f.parent.parent.name.split("=", 1)[1], f.parent.name.split("=", 1)[1])
        for f in gold_dir.glob("year=*/month=*/*.parquet")
    }
    return sorted(partitions)


def run_validation(gold_dir: Path, year: str | None = None, month: str | None = None) -> dict:
    """Corre la consulta con y sin filtro de partición y mide cada una.

    Pre:  gold_dir tiene al menos una partición year=YYYY/month=MM con Parquet.
    Post: dict con la partición medida y, para FULL SCAN y PRUNED, el plan de
          EXPLAIN ANALYZE, los archivos abiertos, las filas leídas y el tiempo.
    Inv:  la consulta es la misma en los dos casos; solo cambia el WHERE.
    """
    # Carpeta inexistente y carpeta vacía son problemas distintos: el primero casi
    # siempre es una ruta mal apuntada (por ejemplo, AIRFLOW_HOME sin definir fuera
    # de Docker), el segundo es que todavía no corrió ningún mes con datos.
    if not gold_dir.is_dir():
        raise FileNotFoundError(
            f"no existe la carpeta {gold_dir}. Pasá la ruta con --gold-dir, por ejemplo "
            f'--gold-dir "<carpeta del docker-compose>/data/gold/{GOLD_DATASET}", '
            "o definí AIRFLOW_HOME apuntando a la carpeta del docker-compose"
        )

    partitions = available_partitions(gold_dir)
    if not partitions:
        raise FileNotFoundError(
            f"gold está vacío: {gold_dir} existe pero no tiene Parquet en "
            f"{Path('year=*') / 'month=*'}. Corré build_gold para algún mes entre 2019-01 y 2024-01"
        )

    if year is None and month is None:
        target = partitions[-1]
    elif year is None or month is None:
        raise ValueError("--year y --month se usan juntos")
    else:
        target = (str(year), str(month).zfill(2))
        if target not in partitions:
            first, last = partitions[0], partitions[-1]
            raise ValueError(
                f"no existe la partición year={target[0]}/month={target[1]}. "
                f"Hay {len(partitions)}, de {first[0]}-{first[1]} a {last[0]}-{last[1]}"
            )

    # as_posix(): DuckDB acepta "/" también en Windows y así no hay que escapar "\".
    # hive_types fuerza year/month a VARCHAR para que '05' compare exacto y no se
    # convierta en el número 5.
    pattern = f"{gold_dir.as_posix()}/**/*.parquet"
    options = "hive_partitioning = true, hive_types = {'year': 'VARCHAR', 'month': 'VARCHAR'}"
    source = f"read_parquet('{pattern}', {options})"
    source_with_file = f"read_parquet('{pattern}', {options}, filename = true)"
    where = f"WHERE year = '{target[0]}' AND month = '{target[1]}'"

    measures = {}
    connection = duckdb.connect()
    try:
        for label, clause in (("FULL SCAN", ""), ("PRUNED", where)):
            sql = QUERY.format(source=source, where=clause)

            start = time.perf_counter()
            connection.execute(sql).fetchall()
            elapsed_ms = (time.perf_counter() - start) * 1000

            plan = connection.execute("EXPLAIN ANALYZE " + sql).fetchall()[0][1]
            # Con filename = true cada fila sabe de qué Parquet salió: contar los
            # distintos muestra cuántos archivos abrió realmente el motor.
            files = connection.execute(
                f"SELECT COUNT(DISTINCT filename) FROM {source_with_file} {clause}"
            ).fetchone()[0]
            rows = connection.execute(f"SELECT COUNT(*) FROM {source} {clause}").fetchone()[0]

            measures[label] = {"plan": plan, "files": files, "rows": rows, "ms": elapsed_ms}
    finally:
        connection.close()

    return {
        "gold_dir": gold_dir,
        "target": target,
        "partitions": len(partitions),
        "full": measures["FULL SCAN"],
        "pruned": measures["PRUNED"],
    }


def side_by_side(left: str, right: str, gap: int = 4) -> str:
    """Pone dos bloques de texto en columnas, línea por línea."""
    left_lines, right_lines = left.splitlines(), right.splitlines()
    width = max((len(line) for line in left_lines), default=0)
    height = max(len(left_lines), len(right_lines))
    left_lines += [""] * (height - len(left_lines))
    right_lines += [""] * (height - len(right_lines))
    return "\n".join(f"{l.ljust(width)}{' ' * gap}{r}".rstrip() for l, r in zip(left_lines, right_lines))


def format_report(result: dict, columns: bool = False) -> str:
    """Arma el reporte: los dos planes (en columnas o uno debajo del otro) y el resumen."""
    full, pruned = result["full"], result["pruned"]
    year, month = result["target"]

    full_block = "=== FULL SCAN (sin filtro de partición) ===\n" + full["plan"]
    pruned_block = f"=== PRUNED SCAN (year = '{year}' AND month = '{month}') ===\n" + pruned["plan"]
    plans = side_by_side(full_block, pruned_block) if columns else f"{full_block}\n\n{pruned_block}"

    saved = full["files"] - pruned["files"]
    reduction = 100 * saved / full["files"] if full["files"] else 0.0

    # "Data Read" del plan = bytes escaneados, que es lo que factura Athena.
    # Algunas versiones de DuckDB no lo informan: en ese caso se muestra "-".
    def data_read(plan: str) -> str:
        match = re.search(r"Data Read:\s*([\d.,]+\s*\w+)", plan)
        return match.group(1) if match else "-"

    return "\n".join([
        f"Dataset    : {result['gold_dir']}",
        f"Particiones: {result['partitions']}",
        f"Filtro     : year = '{year}' AND month = '{month}'",
        f"Generado   : {datetime.now().astimezone().isoformat(timespec='seconds')}",
        "",
        plans,
        "",
        f"{'':<18}{'FULL SCAN':>12}{'PRUNED':>12}",
        f"{'Archivos leídos':<18}{full['files']:>12}{pruned['files']:>12}",
        f"{'Filas leídas':<18}{full['rows']:>12}{pruned['rows']:>12}",
        f"{'Datos leídos':<18}{data_read(full['plan']):>12}{data_read(pruned['plan']):>12}",
        f"{'Tiempo (ms)':<18}{full['ms']:>12.1f}{pruned['ms']:>12.1f}",
        "",
        f"El filtro de partición evitó abrir {saved} de {full['files']} archivos "
        f"({reduction:.1f}% menos).",
    ])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Compara EXPLAIN ANALYZE de gold con y sin filtro de partición."
    )
    parser.add_argument("--gold-dir", type=Path, default=DEFAULT_GOLD_DIR,
                        help=f"carpeta del dataset gold (default: {DEFAULT_GOLD_DIR})")
    parser.add_argument("--year", help="año de la partición a medir, ej. 2023")
    parser.add_argument("--month", help="mes de la partición a medir, ej. 5 o 05")
    parser.add_argument("--output", type=Path, default=DEFAULT_REPORT,
                        help=f"dónde guardar el reporte además de imprimirlo (default: {DEFAULT_REPORT})")
    # Por defecto los planes van uno debajo del otro: cada caja de DuckDB ocupa
    # más de 100 columnas, así que en paralelo solo se leen en una terminal ancha.
    # La tabla de resumen del final siempre compara los dos escenarios en columnas.
    parser.add_argument("--side-by-side", action="store_true",
                        help="muestra los dos planes en columnas (requiere una terminal ancha)")
    args = parser.parse_args(argv)

    # Los planes de DuckDB usan caracteres de caja (┌─┐); en la consola de Windows
    # sin UTF-8 el print fallaría.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    try:
        result = run_validation(args.gold_dir, args.year, args.month)
    except (FileNotFoundError, ValueError) as exc:
        print(f"[finops_validation] {exc}", file=sys.stderr)
        return 1

    report = format_report(result, columns=args.side_by_side)
    print(report)

    # Se sobrescribe en cada corrida: el archivo siempre tiene la última medición.
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report + "\n", encoding="utf-8")
    print(f"\n[finops_validation] reporte guardado en {args.output}")

    if result["pruned"]["files"] >= result["full"]["files"]:
        print(
            f"[finops_validation] el pruning NO funciona: la consulta filtrada abrió "
            f"{result['pruned']['files']} archivos y el scan completo {result['full']['files']}. "
            "Revisar las carpetas year=/month= de gold.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
