# TP Ingeniería de Datos 2026 — Pipeline medallion local

Pipeline bronze → silver → gold orquestado con Airflow y consultado con DuckDB.

## Dataset

kaggle : https://www.kaggle.com/datasets/mustafakeser4/looker-ecommerce-bigquery-dataset?select=products.csv

## Cómo correrlo

Todas las rutas son relativas al repo: después de clonarlo no hay que mover ningún archivo. Hace falta Docker con Compose (Docker Desktop en Windows o Mac).

1. **Datos.** Descargá el dataset de Kaggle y copiá los CSV, con sus nombres originales, en `data/source/`. La carpeta `data/` dentro de tu carpeta docker. Tiene que estar también `events.csv`, porque `extract` copia todos los archivos de `SOURCE_FILES`.
2. **Solo en Linux.** Creá el `.env` para que los archivos que escribe Airflow queden a tu nombre: `echo "AIRFLOW_UID=$(id -u)" > .env`.
3. **Levantar Airflow** desde la raíz del repo: `docker compose up -d --build`. La primera vez construye la imagen con `requirements.txt` e inicializa la base. La UI queda en http://localhost:8080 (usuario y contraseña `airflow`).
4. **Cargar el salt** de la PII: `docker compose exec airflow-scheduler airflow variables set PII_SALT <valor-secreto>`, o desde Admin → Variables. Sin el salt, `mask_pii` falla.
5. **Correr meses con datos.** Activá `medallion_pipeline_dag` en la UI. El dataset va de 2019-01 a 2024-01, así que una corrida con la fecha de hoy no escribe gold. Para cargar, por ejemplo, 2023 completo:

    ```bash
    docker compose exec airflow-scheduler airflow backfill create --dag-id medallion_pipeline_dag --from-date 2023-01-01 --to-date 2023-12-01 --max-active-runs 1
    ```

    `finops_report` se saltea hasta que gold tenga al menos dos meses, porque con una sola partición no hay nada que podar.
6. **Validar desde la PC.** Con `pip install duckdb`, `python scripts/finops_validation.py` lee `data/gold/` y guarda el reporte en `docs/`. Con `pip install duckdb-cli`, `duckdb -f sql/gold_queries.sql` corre las consultas del catálogo.

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

### inventory_items (`clean_inventory_items`)

490.705 filas, una por unidad física de stock.

| Columna | Nulos | Decisión | Justificación de negocio |
|---|---|---|---|
| `sold_at` | 308.946 (63,0%) | Se conservan como `NULL` | La unidad **todavía no se vendió**: sigue en stock. Es el estado normal de un inventario, no un dato faltante. Imputar una fecha la daría por vendida e inflaría cualquier métrica de rotación. |
| `product_name` | 29 (0,006%) | Se conservan | Heredados de `products` (ver más abajo). |
| `product_brand` | 401 (0,08%) | Se conservan | Ídem. |

**Columnas obligatorias.** Se descartan las filas sin `id`, `product_id`, `created_at`, `cost` o `product_distribution_center_id`: sin ellas no se sabe qué unidad es, cuándo entró al stock, cuánto costó ni de qué centro salió. `sold_at` queda afuera a propósito. Hoy se descartan 0 filas.

**Atributos de producto duplicados.** La tabla trae seis columnas `product_*` que repiten datos de `products`. Se verificó sobre las 490.705 filas que **coinciden al 100%** con la tabla de origen (0 diferencias en precio y categoría, 0 `product_id` huérfanos), así que se conservan y se normalizan con el mismo criterio. La fuente de verdad para gold sigue siendo `products`.

**Controles.** `cost` negativo hace **fallar** la tarea. Las unidades con `sold_at` anterior a `created_at` (vendidas antes de existir) se cuentan y se loguean sin eliminarlas: hoy son 0.

### products (`clean_products`)

29.120 filas, el catálogo.

| Columna | Nulos | Decisión | Justificación de negocio |
|---|---|---|---|
| `name` | 2 (0,007%) | Se conservan como `NULL` | Un producto sin nombre comercial cargado **igual se vende**: tiene categoría, precio y SKU. Descartarlo perdería las ventas asociadas en gold. |
| `brand` | 24 (0,08%) | Se conservan como `NULL` | Ídem: no todos los productos son de marca. `NULL` significa "sin marca declarada", que es distinto de inventar un `"Sin marca"` que se confundiría con una marca real. |

**Columnas obligatorias.** Se descartan las filas sin `id`, `cost`, `retail_price`, `category`, `department` o `distribution_center_id`. Sin precio y costo no hay margen que calcular en gold; sin categoría ni departamento el producto no entra en ningún agrupamiento; sin centro de distribución se pierde la trazabilidad logística. Hoy se descartan 0 filas.

**Controles.** Un `cost` o `retail_price` negativo hace **fallar** la tarea. Los productos con precio de venta por debajo del costo se cuentan y se loguean sin eliminarlos: no son un error de datos (puede ser liquidación) pero explican márgenes negativos en gold. Hoy son 0.

**Normalización.** `category`, `department` y `brand` a formato título; `sku` a mayúsculas por ser identificador; `name` solo sin espacios sobrantes.

### distribution_centers (`clean_distribution_centers`)

10 filas, la dimensión más chica. **Sin nulos en ninguna columna.**

**Columnas obligatorias.** Las cuatro (`id`, `name`, `latitude`, `longitude`): en una tabla de 10 filas cada una se usa entera, el id para unir y las coordenadas para cualquier análisis geográfico.

**Control de rango.** Si aparece una latitud fuera de ±90 o una longitud fuera de ±180, la tarea **falla**. En una dimensión de 10 filas una coordenada imposible es un error de carga, no un outlier estadístico.

**Normalización.** A `name` solo se le quitan espacios, **no** se le aplica formato título: los nombres llevan la sigla del estado (`"Memphis TN"`) y pasarlos a título la rompería (`"Memphis Tn"`).

### users (`clean_users`)

100.000 filas. Es la única tabla con **PII**, así que su tarea no escribe en silver: deja el resultado en `staging/`, y `mask_pii` es la que anonimiza y publica en silver.

| Columna | Nulos | Decisión | Justificación de negocio |
|---|---|---|---|
| `city` | 958 (1,0%) | Se conservan como `NULL` | Un domicilio incompleto no invalida al cliente ni a sus compras. El usuario sigue teniendo `state` y `country` para los análisis geográficos agregados. |

**Columnas obligatorias.** Se descartan las filas sin `id`, `email` o `created_at`. El `email` es obligatorio porque es lo que `mask_pii` convierte en `email_hash`, la clave con la que un analista agrupa clientes en gold: sin él, el usuario no se puede unir a nada. Hoy se descartan 0 filas.

**No se deduplica por email.** 15.989 usuarios (16%) comparten dirección de correo con otro usuario. Deduplicar por email fusionaría clientes distintos y perdería sus compras. La identidad de negocio es `id`; el hash del email es un atributo, no una clave. **Consecuencia a tener presente:** `email_hash` no identifica unívocamente a un cliente en gold.

**Normalización del email antes del hash.** El email se lleva a minúsculas y sin espacios **en esta tarea**, no en `mask_pii`. El hash se calcula sobre el texto final, así que `"Juan@Mail.com"` y `"juan@mail.com"` tienen que llegar idénticos o producirían dos hashes distintos para la misma dirección.

**Normalización del resto.** `gender` a mayúsculas; `state`, `city`, `country` y `traffic_source` a formato título. A `first_name`, `last_name` y `street_address` solo se les quitan espacios: son PII que `mask_pii` va a descartar, y aplicarles formato título alteraría apellidos legítimos.

## Gobernanza de PII (`mask_pii`)

`users` es la única tabla con datos personales, y por eso su limpieza está partida en dos tareas: `clean_users` escribe en `staging/` (fuera de silver) y `mask_pii` es la única que publica en `silver/users/`. El email en claro nunca se escribe en la capa que consultan los analistas.

De las 15 columnas originales quedan 9:

| Columna | Tratamiento | Por qué |
|---|---|---|
| `email` | **Hash** SHA-256 + salt → `email_hash` | Es la clave con la que un analista agrupa clientes. Hay que poder unir por ella sin exponer la dirección. |
| `first_name`, `last_name` | **Se borran** | Nadie agrupa ventas por nombre. Hashearlos tampoco serviría: hay pocos miles de nombres posibles y la tabla de hashes se arma en minutos. |
| `street_address` | **Se borra** | Identifica un domicilio y no entra en ningún agregado. |
| `postal_code` | **Se borra** | **2.515 usuarios son los únicos con su código postal**: para ellos el código *es* un identificador. |
| `latitude`, `longitude` | **Se borran** | 16.404 pares distintos para 100.000 usuarios. Conservarlos mientras se borra la dirección sería contradictorio: son dos formas de decir lo mismo. |
| `id`, `age`, `gender`, `state`, `city`, `country`, `traffic_source`, `created_at` | **Se conservan** | Atributos demográficos agregables: sirven para analizar y no apuntan a una persona concreta. `city`, `state` y `country` alcanzan para cualquier análisis geográfico. |

**Por qué hash y no borrado, solo para el email.** El hash tiene una única utilidad: conservar la capacidad de unir o agrupar sin exponer el valor. Si no se necesita unir por esa columna, el hash no aporta nada y el borrado protege más. El email es la excepción porque *es* el identificador de cliente.

**Por qué el salt.** Sin salt, un SHA-256 de un email se revierte por diccionario: el espacio de direcciones plausibles es chico y precalcular sus hashes es barato. Con un salt secreto que no vive junto a los datos, esa tabla precalculada no sirve. El salt se guarda como **Airflow Variable** `PII_SALT`, nunca en un archivo del repo, y se lee **dentro de la tarea** (`Variable.get` a nivel módulo se ejecutaría en cada parseo del DAG, cada ~30 segundos, golpeando la metadata DB sin motivo). Si la Variable no está, la tarea falla con un mensaje explicando cómo cargarla: no hay hash sin salt.

**Verificación.** Sobre las 100.000 filas: 0 emails nulos, hashes de 64 caracteres, y **84.011 hashes distintos**. La diferencia con las 100.000 filas son exactamente los 15.989 usuarios que comparten email: mismo email → mismo hash, que es la propiedad que hace el hash joinable. El único nulo que sobrevive en silver es `city` (958).

**Control de fugas.** Después del borrado, la tarea verifica que ninguna columna de PII quedó en el DataFrame antes de escribir, y recién ahí borra `staging/users/`. Si mañana el origen agrega una columna sensible y alguien olvida sumarla a la lista, el chequeo la detecta.

**Limitación a tener presente.** Como 15.989 usuarios comparten email, `email_hash` **no identifica unívocamente a un cliente**: agrupar por hash puede juntar dos usuarios distintos. La identidad de negocio sigue siendo `id`.

## Compuerta de integridad (`validate_silver`)

Antes de construir gold, se verifica que las tablas de silver se puedan unir sin perder ni multiplicar filas. La tarea distingue dos clases de problema:

- **Unicidad de claves primarias → corta la corrida.** Una PK repetida multiplica filas en el join de gold y rompe el grano del modelo *en silencio*: la suma de ingresos daría más alta sin que nada falle. Es el tipo de error que no se puede dejar pasar.
- **Integridad referencial → se loguea.** Un huérfano es un dato a investigar, pero qué hacer con él es una decisión de modelado que toma `build_gold` (unir con `INNER` o con `LEFT`), no la compuerta.

Las cinco relaciones que se verifican son exactamente los joins que arma gold:

```
order_items.order_id          → orders.order_id
order_items.user_id           → users.id
order_items.product_id        → products.id
order_items.inventory_item_id → inventory_items.id
products.distribution_center_id → distribution_centers.id
```

**Resultado medido sobre los datos actuales:**

| Tabla | Filas | PK |
|---|---|---|
| `orders` | 125.226 | `order_id` única |
| `order_items` | 181.759 | `id` única |
| `products` | 29.120 | `id` única |
| `inventory_items` | 490.705 | `id` única |
| `distribution_centers` | 10 | `id` única |
| `users` | 100.000 | `id` única |

**0 huérfanos en las cinco relaciones** (0,00% en cada una). Esto es lo que habilita usar `INNER JOIN` en gold sin perder ventas: está verificado, no asumido.

**Costo.** La tarea lee únicamente las columnas de claves, no las tablas completas: validar la integridad de `inventory_items` no requiere cargar sus 490.705 registros con los doce campos.

## Capa Gold (`build_gold`)

Modelo analítico denormalizado. **Grano: un registro por ítem vendido.**

### Join de cinco entidades de silver

```
order_items                                   (grano)
   ├── orders               order_id          INNER
   ├── users                user_id           LEFT
   ├── products             product_id        LEFT
   └── distribution_centers  vía products.distribution_center_id   LEFT
```

**Por qué `INNER` contra `orders` y `LEFT` contra las dimensiones.** Un ítem sin pedido no es una venta: no tiene fecha, ni estado, ni cliente, y no debería existir en gold. En cambio, un producto sin nombre cargado o un centro de distribución faltante no pueden hacer desaparecer una venta real. `validate_silver` reporta hoy 0 huérfanos en las cinco relaciones, así que ambas decisiones dan el mismo resultado; la diferencia protege de cargas futuras.

**`inventory_items` queda fuera del join.** Se verificó que `inventory_items.cost` coincide con `products.cost` en **las 181.759 filas** (0 diferencias), así que el costo unitario sale de `products` sin perder precisión y el modelo se simplifica en una tabla.

### Particionado

```
data/gold/order_items_enriched/year=YYYY/month=MM/order_items_enriched.parquet
```

**La fecha de negocio es `orders.created_at`**, la fecha en que se hizo el pedido. Se eligió sobre las alternativas:

- `delivered_at`: nula en el 65% de los casos, dejaría la mayoría de las ventas sin partición.
- `order_items.created_at`: no coincide con la de su pedido (la fila del ítem se escribe días después), así que partiría pedidos entre dos meses.
- Dimensiones como categoría o centro: no son temporales, no sirven para backfills ni para consultas por período.

**`year` y `month` no se escriben dentro del Parquet.** Viven solo en el nombre de la carpeta y DuckDB los reconstruye como columnas con `hive_partitioning=true`. Escribirlos además sería duplicar el dato en cada fila.

**El ítem se asigna al mes de su orden, no al suyo propio:** el filtro es por `order_id` contra las órdenes del mes, no por `order_items.created_at`.

### Idempotencia

Antes de escribir, la tarea borra **esa** carpeta de partición y la vuelve a crear. Nunca hace append, y nunca toca los otros meses. Correr dos veces el mismo mes deja exactamente el mismo contenido. Además, un control verifica que ninguna fila quede fuera de la ventana antes de escribir.

### Columnas

30 columnas: claves, fechas del ciclo de vida, estados, importes (`sale_price`, `product_cost`, `product_retail_price`), atributos de producto y de logística, y datos del cliente ya anonimizados (`user_email_hash`, edad, género, ubicación, canal).

Tres métricas derivadas que el analista no debería tener que calcular: `margin` (`sale_price - product_cost`), `is_returned` y `delivery_days`.

### Resultado medido (mayo 2023)

```
4093 órdenes → 5948 ítems | 0 filas fuera de la ventana
ingresos 347.730,12 | margen 180.249,28 | devueltos 630
días de entrega promedio: 4.0
```

Un `product_name` quedó nulo: es uno de los dos productos sin nombre que `clean_products` decidió conservar. El `LEFT JOIN` hizo su trabajo — la venta está en gold aunque al producto le falte el nombre comercial.

### Una corrida llena una sola partición

Cada ejecución escribe **un mes**. Para tener las 61 particiones del dataset (2019-01 a 2024-01) hay que hacer un **backfill** de ese rango. Si la fecha lógica cae fuera —por ejemplo una corrida manual de hoy— no hay órdenes que procesar y la partición queda vacía; la tarea lo avisa en el log en vez de fallar en silencio.

### Compuerta de gold (`validate_gold`)

Verifica la partición recién escrita, con cuatro controles que **cortan la corrida**:

1. **Grano único.** Ningún `order_item_id` repetido. Un duplicado acá significa que algún join multiplicó filas y los ingresos salen inflados sin que nada más lo delate.
2. **Ventana.** Si la carpeta dice `year=2023/month=10`, todas las filas tienen que tener `ordered_at` en octubre 2023.
3. **Claves y métricas sin nulos** en `order_item_id`, `order_id`, `user_id`, `ordered_at` y `sale_price`. Gold es lo que consume el analista: un nulo acá rompe cualquier agregación.
4. **Gobernanza.** Ninguna columna de PII en claro (`email`, nombres, dirección, código postal, coordenadas) puede haber llegado hasta gold. Es la última barrera, después de `mask_pii`.

**Huella del contenido.** Además del conteo, la tarea calcula un SHA-256 de la partición ordenada por `order_item_id` y lo loguea:

```
[validate_gold] 2023-10 OK | 8916 ítems | 6108 órdenes | 30 columnas
[validate_gold] ingresos 544,667.37 | margen 282,993.96 | devueltos 929
[validate_gold] huella del contenido: 7034b82274018adb
```

Eso es evidencia más fuerte que el conteo para el test de idempotencia: dos corridas del mismo mes pueden dar la misma cantidad de filas con contenido distinto, pero no pueden dar la misma huella. Si los dos intentos loguean `7034b82274018adb`, las particiones son idénticas byte a byte en sus datos.

**Verificado** sobre dos particiones reales: `2023-10` (8.916 ítems) y `2023-05` (5.948 ítems), ambas con 0 duplicados, 0 filas fuera de ventana, 0 nulos y 0 columnas de PII.

## Catálogo local DuckDB (`register_duckdb`)

Expone gold a DuckDB creando una vista sobre las particiones:

```sql
CREATE OR REPLACE VIEW gold_order_items_enriched AS
SELECT * FROM read_parquet(
    '.../gold/order_items_enriched/**/*.parquet',
    hive_partitioning = true,
    hive_types = {'year': 'VARCHAR', 'month': 'VARCHAR'}
);
```

**La vista no copia datos: guarda la consulta.** El archivo `catalog.duckdb` pesa 274 KB mientras gold tiene 181.759 filas, porque lo único que almacena es ese texto SQL con un nombre. Cada `SELECT` sobre la vista lee los Parquet en ese momento, así que una partición nueva aparece sola sin volver a registrar nada.

**`hive_types` fuerza `year` y `month` a VARCHAR.** Sin eso, DuckDB autocastea los valores de las carpetas a entero y `month = '05'` dejaría de coincidir, porque `05` se convertiría en `5`. Con el tipo fijado, los filtros del enunciado funcionan tal cual están escritos.

**`CREATE OR REPLACE`** hace la tarea idempotente: corre en cada ejecución del DAG sin fallar por "la vista ya existe".

**Resultado medido:**

```
[register_duckdb] vista 'gold_order_items_enriched' creada en /opt/airflow/data/catalog.duckdb
[register_duckdb] 61 particiones (2019..2024) | 181759 filas | 125226 órdenes | ingresos 10,827,118.91
```

Y el filtro exacto devuelve lo esperado: `WHERE year='2023' AND month='05'` → 5.948 filas.

**Nota sobre rutas.** La vista guarda la ruta absoluta del contenedor (`/opt/airflow/data/...`), así que el catálogo se consulta desde adentro de Airflow. Desde Windows se consulta apuntando directo a los Parquet con la ruta del host, que es lo que hacen `sql/gold_queries.sql` y `scripts/finops_validation.py`.

**Comparación con AWS.** En Athena esta tarea no sería opcional: el catálogo de Glue es una lista explícita de particiones y habría que registrarlas con `MSCK REPAIR TABLE` o un Crawler. DuckDB resuelve el patrón `**/*.parquet` en el momento de la consulta, así que descubre las particiones nuevas solo.

## Validación FinOps (`finops_report`)

Corre **la misma agregación dos veces** —sin filtro de partición y con `WHERE year = '…' AND month = '…'`— y compara qué leyó el motor en cada caso. Lo único que cambia entre las dos consultas es el `WHERE`.

**Resultado medido** sobre las 61 particiones:

| | FULL SCAN | PRUNED |
|---|---|---|
| Archivos Parquet leídos | **61** | **1** |
| Filas leídas | 181.759 | 5.948 |
| Tiempo | 1.353,7 ms | 1.189,1 ms |

El filtro evitó abrir **60 de 61 archivos (98,4% menos)**. Y DuckDB lo confirma en el nodo `TABLE_SCAN` de su propio plan:

```
   File Filters:
   (year = '2023' AND month = '05')

Scanning Files: 1/62
```

**Cómo se mide la cantidad de archivos.** Con `filename = true`, cada fila sabe de qué Parquet salió; contar los distintos prueba cuántos abrió el motor realmente. Es más directo que interpretar el plan.

**La métrica que importa es la cantidad de archivos, no el tiempo.** A esta escala (181.759 filas, 13 MB) el tiempo está dominado por el arranque de la consulta y las dos cifras quedan parecidas. Lo que se está demostrando es el principio: el motor **descarta archivos sin abrirlos** gracias a la estructura de carpetas. En Athena, esos 60 archivos que no se abren son 60 archivos que no se facturan.

**La tarea falla si el pruning no funciona.** Si la consulta filtrada leyera la misma cantidad de archivos que el scan completo, el particionado estaría mal y `finops_report` corta la corrida en vez de reportar un resultado inútil.

El reporte completo con los dos `EXPLAIN ANALYZE` queda en `data/finops_report.txt`, y se reescribe en cada corrida.

> **Pendiente:** hay una partición vacía `year=2024/month=02` de una corrida anterior. Por eso el plan dice `1/62` en vez de `1/61`. Hay que borrar esa carpeta.

### events: fuera del alcance

`events.csv` (2.431.963 filas, 376 MB) registra la navegación de los usuarios por el sitio: clics, vistas de producto y carritos. No alimenta el modelo de gold, que se construye sobre ventas concretas. Se excluye del pipeline de silver, lo que además deja el dataset en 139 MB, dentro del límite de 50–300 MB de la consigna.
