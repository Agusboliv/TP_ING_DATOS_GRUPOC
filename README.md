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

- **Descarte por nulos en columnas críticas:** se eliminan las filas con nulo en `order_id`, `user_id`, `created_at` o `status`. Son las cuatro columnas sin las cuales la fila no sirve: sin `order_id` la orden no se puede unir a sus ítems, sin `user_id` no se puede atribuir a un cliente, sin `created_at` no hay fecha de negocio que la ubique en una partición de gold, y sin `status` no se puede evaluar la regla de coherencia. **`gender` y `num_of_item` quedan fuera de esta lista a propósito:** una orden sin género declarado o sin cantidad sigue siendo una venta válida y contable, y descartarla perdería facturación real. El descarte se hace antes de deduplicar, para que un `order_id` nulo no compita como clave. Hoy se descartan 0 filas; la regla protege contra cargas futuras.
- **Deduplicación:** se eliminan duplicados por la clave primaria `order_id`. El origen no tiene duplicados, pero la regla se aplica en cada corrida.
- **Tipado:** las cuatro fechas pasan de texto a `timestamp` UTC con precisión de microsegundos. El CSV mezcla dos formatos (con y sin microsegundos) y se parsean ambos con ISO 8601. `order_id` y `user_id` pasan a `int64` y `num_of_item` (valores 1 a 4) a `int8`.
- **Normalización:** `status` se lleva a formato título y `gender` a mayúsculas, quitando espacios. El origen ya viene limpio; la regla protege contra cargas futuras con casing inconsistente.

### order_items (`clean_order_items`)

181.759 filas. Es el grano del modelo de gold: un registro por ítem vendido. Igual que en `orders`, los únicos nulos están en las tres fechas de avance; `id`, las cuatro claves foráneas (`order_id`, `user_id`, `product_id`, `inventory_item_id`), `status`, `created_at` y `sale_price` están completos.

| Columna | Nulos | Decisión | Justificación de negocio |
|---|---|---|---|
| `shipped_at` | 63.478 (34,9%) | Se conservan como `NULL` | El ítem todavía no fue despachado: status `Processing` o `Cancelled`. |
| `delivered_at` | 117.918 (64,9%) | Se conservan como `NULL` | El ítem no llegó al cliente: `Processing`, `Cancelled` o `Shipped` (en tránsito). |
| `returned_at` | 163.527 (90,0%) | Se conservan como `NULL` | Solo los ítems devueltos tienen fecha de devolución. |

**Evidencia.** El ítem hereda el ciclo de vida de su orden, así que se aplica la **misma regla status ↔ fechas** que en `orders` y se verifica en cada corrida: sobre las 181.759 filas hay **0 incoherencias**. Las proporciones de nulos coinciden con las de `orders` (34,9% / 65,0% / 90,0%), lo que confirma que ambos niveles cuentan la misma historia.

**Alternativas descartadas.** Las mismas que en `orders`: eliminar las filas con nulos descartaría el 90% de las ventas, y una fecha centinela inventaría despachos y entregas que nunca ocurrieron, distorsionando cualquier métrica de logística.

**Otras reglas de calidad en order_items.**

- **Descarte por nulos en columnas críticas:** se eliminan las filas con nulo en `id`, `order_id`, `user_id`, `product_id`, `inventory_item_id`, `created_at` o `status`. Acá entran **las cinco claves**, porque el grano de gold es el ítem vendido: sin alguna de ellas el ítem no se puede atribuir a una orden, un cliente, un producto o una unidad de inventario, y deja de ser un registro de venta utilizable. `sale_price` no entra en esta lista: se valida aparte y hace **fallar** la tarea, porque descartar en silencio un ítem sin precio alteraría los ingresos de gold sin que nadie se entere. El descarte se hace antes de deduplicar. Hoy se descartan 0 filas.
- **Deduplicación:** por la clave primaria `id`. El origen no trae duplicados, pero la regla se aplica igual en cada corrida.
- **Tipado de claves:** `id` y las cuatro foráneas pasan a `int64`, el mismo tipo que usan `orders.order_id` y `users.id` en sus tablas silver. Unir claves de tipos distintos es una fuente silenciosa de filas perdidas en gold, así que el tipo se fija acá y no en el join.
- **Tipado de importes:** `sale_price` pasa a `float64`. Es el único valor monetario de la tabla y la base de los ingresos y el margen en gold.
- **Validación de rango:** la tarea falla si aparece algún `sale_price` nulo o negativo. Una venta con importe negativo no es un dato sucio recuperable: es un error de origen que invalidaría las sumas de gold. Hoy hay 0 casos.
- **Normalización:** `status` se lleva a formato título quitando espacios, igual que en `orders`.

### Pendientes

`inventory_items`, `products`, `distribution_centers`, `events`, `users`: se documentan al implementar su tarea `clean_*`.
