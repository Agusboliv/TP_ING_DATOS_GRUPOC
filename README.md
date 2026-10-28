# TP Ingeniería de Datos 2026 — Pipeline medallion local

Pipeline bronze → silver → gold orquestado con Airflow y consultado con DuckDB.

## Dataset

kaggle : https://www.kaggle.com/datasets/mustafakeser4/looker-ecommerce-bigquery-dataset?select=products.csv

## Manejo de nulos y calidad de datos (silver)

Cada decisión sobre nulos queda documentada con su justificación de negocio. Las tablas se van agregando a medida que se implementa su tarea `clean_*`.

### orders (`clean_orders`)

125.226 filas. Solo hay nulos en las tres fechas de avance de la orden; el resto de las columnas (`order_id`, `user_id`, `status`, `gender`, `created_at`, `num_of_item`) está completo.

| Columna | Nulos | Decisión | Justificación de negocio |
|---|---|---|---|
| `shipped_at` | 43.765 (34,9%) | Se conservan como `NULL` | La orden todavía no fue despachada: corresponde a los status `Processing` y `Cancelled`. No es un dato faltante, es un evento que no ocurrió. |
| `delivered_at` | 81.342 (65,0%) | Se conservan como `NULL` | La orden no llegó al cliente: `Processing`, `Cancelled` o `Shipped` (en tránsito). |
| `returned_at` | 112.696 (90,0%) | Se conservan como `NULL` | Solo las órdenes devueltas tienen fecha de devolución; las demás nunca se devolvieron. |

**Evidencia.** Los nulos coinciden al 100% con el `status` de la orden (verificado sobre las 125.226 filas):

| status | `shipped_at` | `delivered_at` | `returned_at` |
|---|---|---|---|
| `Processing`, `Cancelled` | nulo | nulo | nulo |
| `Shipped` | con fecha | nulo | nulo |
| `Complete` | con fecha | con fecha | nulo |
| `Returned` | con fecha | con fecha | con fecha |

Además, las fechas respetan el orden del proceso: ningún envío es anterior a la creación, ninguna entrega es anterior al envío y ninguna devolución es anterior a la entrega.

**Alternativas descartadas.**

- **Eliminar las filas con nulos:** se perdería el 90% de la tabla (todas las órdenes no devueltas), incluidas órdenes completas y válidas.
- **Imputar una fecha centinela** (por ejemplo `1900-01-01` o `9999-12-31`): inventaría eventos que no ocurrieron y distorsionaría métricas como el tiempo promedio de entrega. El estado de la orden ya está en `status`, así que la fecha centinela no aporta información.
- **Guardar las fechas como texto vacío:** la columna perdería el tipo fecha. Parquet y DuckDB admiten `NULL` en columnas `timestamp` de forma nativa, así que el nulo conserva el tipo.

**Control en cada corrida.** `clean_orders` verifica la regla status ↔ fechas: si aparece un `status` desconocido la tarea falla (cambiaron las reglas del negocio), y si hay filas donde el status no coincide con las fechas las cuenta en el log sin eliminarlas.

**Otras reglas de calidad en orders.**

- **Deduplicación:** se eliminan duplicados por la clave primaria `order_id`. El origen no tiene duplicados, pero la regla se aplica en cada corrida.
- **Tipado:** las cuatro fechas pasan de texto a `timestamp` UTC con precisión de microsegundos. El CSV mezcla dos formatos (con y sin microsegundos) y se parsean ambos con ISO 8601. `order_id` y `user_id` pasan a `int64` y `num_of_item` (valores 1 a 4) a `int8`.
- **Normalización:** `status` se lleva a formato título y `gender` a mayúsculas, quitando espacios. El origen ya viene limpio; la regla protege contra cargas futuras con casing inconsistente.

### Pendientes

`order_items`, `inventory_items`, `products`, `distribution_centers`, `events`, `users`: se documentan al implementar su tarea `clean_*`.
