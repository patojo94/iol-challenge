# Decisiones Técnicas

Registro de las decisiones de diseño del pipeline, en formato ADR (*Architecture Decision Record*) simplificado.
Cada decisión documenta el contexto, la alternativa elegida, las descartadas y sus tradeoffs, para que el equipo que mantenga el pipeline entienda **por qué** está construido así.

**Estados posibles:** `Propuesta` · `Aceptada` · `Reemplazada por DT-XXX` · `Descartada`

## Índice

| ID | Título | Área | Estado |
|---|---|---|---|
| [DT-001](#dt-001-no-versionar-el-dataset-original) | No versionar el dataset original | Repositorio / Seguridad | Aceptada |
| [DT-002](#dt-002-template-de-variables-de-entorno) | Template de variables de entorno | Repositorio / Seguridad | Aceptada |
| [DT-003](#dt-003-bronze-como-copia-fiel-de-la-fuente) | Bronze como copia fiel de la fuente | Bronze | Aceptada |
| [DT-004](#dt-004-tratamiento-de-operaciones-en-días-inhábiles) | Tratamiento de operaciones en días inhábiles | Calidad / Silver | Aceptada |
| [DT-005](#dt-005-identificación-de-variantes-de-liquidación-mep--cable) | Identificación de variantes de liquidación (MEP / Cable) | Calidad / Enriquecimiento | Aceptada |
| [DT-006](#dt-006-detección-de-montos-y-precios-extremos) | Detección de montos y precios extremos | Calidad | Aceptada |
| [DT-007](#dt-007-segmentación-de-clientes-ante-distribución-asimétrica) | Segmentación de clientes ante distribución asimétrica | Gold | Aceptada |
| [DT-008](#dt-008-schema-landing-propio-para-archivos-crudos) | Schema `landing` propio para archivos crudos | Arquitectura | Aceptada |
| [DT-009](#dt-009-notebooks-como-disparadores-lógica-en-src) | Notebooks como disparadores, lógica en `src/` | Arquitectura / Testing | Aceptada |
| [DT-010](#dt-010-orquestación-con-notebook-en-lugar-de-databricks-jobs) | Orquestación con notebook en lugar de Databricks Jobs | Orquestación | Aceptada |
| [DT-011](#dt-011-clasificación-de-instrumentos-por-reglas) | Clasificación de instrumentos por reglas | Silver | Aceptada |
| [DT-012](#dt-012-silver-se-reconstruye-completo-en-cada-corrida) | Silver se reconstruye completo en cada corrida | Silver / Escalabilidad | Aceptada |
| [DT-013](#dt-013-outputs-públicos-sin-datos-a-nivel-registro) | Outputs públicos sin datos a nivel registro | Seguridad | Aceptada |
| [DT-014](#dt-014-registro-declarativo-de-reglas-de-calidad) | Registro declarativo de reglas de calidad | Calidad | Aceptada |
| [DT-015](#dt-015-modelo-en-estrella-con-claves-subrogadas-deterministas) | Modelo en estrella con claves subrogadas deterministas | Gold | Aceptada |
| [DT-016](#dt-016-scd-tipo-2-en-dim_cliente-con-cortes-mensuales) | SCD Tipo 2 en `dim_cliente` con cortes mensuales | Gold | Aceptada |

## Pendientes

- [ ] Escribir el `README.md`: diagrama de arquitectura, instrucciones de reproducción, decisiones con tradeoffs (enlazando este documento), tabla de cobertura de cotizaciones por símbolo y documentación del componente de IA.
- [x] Definir el mecanismo de idempotencia de la ingesta a Bronze: Auto Loader (DT-003).
- [x] Definir la ruta del volumen de Unity Catalog donde se aloja el CSV (DT-008).
- [x] Calibrar el umbral N de desvío de precio contra la mediana (DT-006): N = 10.
- [ ] Evaluar si se versiona una muestra ofuscada del dataset (DT-001).
- [ ] Activar "Commit notebook outputs" en el Git folder de Databricks.
- [ ] Ejecutar setup y notebooks en Databricks y commitear los `.ipynb` con outputs.
- [ ] Documentar en arquitectura el Job productivo equivalente al orquestador (DT-010).
- [ ] Consultar a IOL si `fecha` está realmente en UTC (las horas cubren las 24 hs) (DT-004).
- [ ] Confirmar el horario de mercado de BYMA contra la fuente oficial (hoy 11:00 a 17:00 ART en `MARKET_OPEN_HOUR` / `MARKET_CLOSE_HOUR`), incluyendo preapertura y diferencias por segmento (DT-004).
- [ ] Extender `FERIADOS_AR` si se procesan períodos fuera de ene-mar 2026 (DT-004).
- [ ] Preguntar a IOL si puede compartir un maestro de clientes (perfil de inversor, fecha de alta) para sumar atributos a `dim_cliente` (DT-007).
- [ ] Clasificar con IA los instrumentos que caen en `ACCION` por descarte (DT-011, Task 07).

---

## DT-001: No versionar el dataset original

- **Estado:** Aceptada
- **Fecha:** 2026-10-15

**Contexto.** La consigna prohíbe subir los datos originales al repositorio público. El CSV provisto (`data-set-challenge-5-.csv`, 100k filas) contiene identificadores de clientes y transacciones.

**Decisión.** La carpeta `demo-data/` se excluye vía `.gitignore`. El README incluirá instrucciones para ubicar el archivo localmente y subirlo al volumen de Databricks.

**Alternativas consideradas.**
- *Versionar una muestra ofuscada:* queda pendiente como mejora, porque permitiría reproducir el pipeline sin pedir el archivo.

**Consecuencias.** Para reproducir el pipeline hace falta obtener el dataset por fuera del repo.

---

## DT-002: Template de variables de entorno

- **Estado:** Aceptada
- **Fecha:** 2026-10-15

**Contexto.** El pipeline necesita credenciales (por ejemplo `DATABRICKS_TOKEN`) que no deben versionarse.

**Decisión.** Se versiona `.env.dev` con las claves y **sin valores**, como estructura de referencia. El `.env` real queda excluido en `.gitignore`.

**Consecuencias.** Quien clone el repo copia `.env.dev` a `.env` y completa los valores. Cada variable nueva debe agregarse también al template.

---

## DT-003: Bronze como copia fiel de la fuente

- **Estado:** Aceptada
- **Fecha:** 2026-10-15

**Contexto.** La consigna exige trazabilidad total: los registros con problemas de calidad **no se descartan**, se marcan y se registran. Además, el pipeline debe ser idempotente.

**Decisión.**
- Bronze conserva los nombres y valores de las columnas originales, más metadatos de ingesta (`_source_file`, `_file_modified_at`, `_ingested_at`, `_rescued_data`). Las reglas de calidad se aplican en Silver, donde cada registro lleva sus flags (por ejemplo `dq_flags ARRAY<STRING>`).
- La ingesta usa **Auto Loader con `trigger(availableNow=True)`**: procesa los archivos pendientes y termina, como un batch.
- **Schema explícito**, sin inferencia, con montos en `DECIMAL(20,6)`. Los valores que no castean quedan en `NULL` y el original se guarda en `_rescued_data`.
- **Idempotencia en dos niveles:** por archivo, mediante el checkpoint de Auto Loader; por registro, deduplicando por `id_transaccion` en Silver (Bronze no deduplica porque es la copia fiel).
- Cada corrida usa un `_ingested_at` fijo, que la identifica. El notebook valida que las filas escritas coincidan con las de los archivos procesados (status `OK` / `WARN` / `FAIL`) y re-ejecuta la ingesta para verificar que escribe 0 filas.

**Alternativas consideradas.**
- *`spark.read` + overwrite:* es idempotente, pero reescribe todo en cada corrida y se pierde el historial de cargas.
- *`COPY INTO`:* también es idempotente por archivo y más simple. Se descartó porque Auto Loader escala mejor a ingesta incremental diaria (el caso productivo) y soporta evolución de schema.
- *Marcar calidad directamente en Bronze:* mezcla datos crudos con lógica de negocio, y cambiar una regla obligaría a reprocesar la capa raw.
- *Tabla de cuarentena separada:* fragmenta el dato. Se prefiere un flag sobre la misma tabla más una vista de registros con problemas.
- *Schema inferido:* es riesgoso en datos financieros, porque `cantidad` llega a 1.500M y desbordaría un `INT`.

**Consecuencias.** Si se borra el checkpoint, los archivos se re-ingieren y Bronze tendría duplicados. Lo detecta y resuelve la deduplicación de Silver.

---

## DT-004: Tratamiento de operaciones en días inhábiles

- **Estado:** Aceptada
- **Fecha:** 2026-10-15

**Contexto (perfilado).**
- 12.498 operaciones caen en **sábado** UTC, todos los sábados del período. No hay operaciones en domingo ni en los feriados (01/01, 16/02 y 17/02).
- Las horas UTC cubren las 24 horas del día, aunque el horario de BYMA es de 11 a 17 ART. Solo el **27,6%** de las operaciones ocurre en día hábil y dentro del horario de mercado.
- Interpretación: son órdenes cargadas en canales 24/7, que se ejecutan en la siguiente rueda.

**Decisión.**
- `fecha` se toma como UTC, tal como indica la fuente (`Z`). Se derivan `fecha_hora_local` y `fecha_local` en `America/Argentina/Buenos_Aires`, porque el mercado es argentino. En hora local, las operaciones en día inhábil son 10.255.
- `fecha_operativa` = `fecha_local` si es hábil; si no, el **siguiente día hábil**. Las cotizaciones (Task 04) se buscan por `fecha_operativa`.
- Flag `DIA_INHABIL` en `dq_flags`; la fila no se descarta.
- `en_horario_mercado` es un atributo informativo, **no un flag**: operar fuera de horario es el comportamiento normal del 62% de las órdenes y no indica un problema de calidad. Las órdenes fuera de horario en día hábil **no** se mueven al día siguiente, para no imputar sobre un supuesto no confirmado.
- Los feriados viven en `FERIADOS_AR` (`src/iol_pipeline/silver.py`), que es la única fuente de verdad, y se publican en `silver.ref_feriados`. Cubren el período del dataset.

**Alternativas consideradas.**
- *Descartar operaciones de sábado:* viola el requisito de trazabilidad.
- *Usar el último cierre anterior:* para una orden cargada fuera de horario, el precio relevante es el de la próxima rueda.
- *Calcular días inhábiles sobre la fecha UTC:* clasificaría como sábado operaciones del viernes por la noche en Argentina.

**Consecuencias.** Si IOL confirma que `fecha` no está en UTC, solo cambia la derivación de `fecha_hora_local` (una línea). El calendario hay que extenderlo para procesar otros períodos.

---

## DT-005: Identificación de variantes de liquidación (MEP / Cable)

- **Estado:** Aceptada (revisa la propuesta original)
- **Fecha:** 2026-10-15

**Contexto (perfilado).**
- Todos los símbolos en USD (471) terminan en `D` (441, MEP) o en `C` (30, Cable).
- Ningún símbolo en ARS es una variante: los que terminan en D o C (`YPFD`, `GLD`, `AMD`, `INTC`) son tickers reales.
- La propuesta original exigía que el ticker base existiera en el dataset, pero **falla en el 41% de los casos**: 192 variantes en USD no tienen su base operada en ARS en el período (por ejemplo `BA7DD`, `BPA8C`, `CS38D`).

**Decisión.** La moneda es el discriminador. Si `moneda = 'USD'`, la operación es una variante: sufijo `D` → `MEP`, `C` → `CABLE` y `simbolo_base` = el símbolo sin el último carácter. En ARS, `tipo_liquidacion = 'ARS'` y `simbolo_base = simbolo`. La existencia de la base en el dataset se reporta como métrica de cobertura, no como condición.

**Alternativas consideradas.**
- *Solo por sufijo:* clasificaría mal tickers como YPFD o GLD.
- *Sufijo + base existente en el dataset (propuesta original):* deja sin clasificar el 41% de las variantes.
- *Tabla de mapeo manual:* no hace falta, porque la regla por moneda no tiene excepciones en los datos.

**Consecuencias.** Un símbolo en USD sin sufijo D/C se clasificaría como `USD_OTRO`. Hoy no hay ninguno, y el caso queda visible en el notebook.

---

## DT-006: Detección de montos y precios extremos

- **Estado:** Aceptada
- **Fecha:** 2026-10-15

**Contexto (perfilado).**
- **Precios placeholder:** valores de `0.01` o cercanos a `1.00` en instrumentos con medianas muy superiores (`AL30` tiene mediana de 876 y mínimo de 0.01; también `SATL`, `TGNO4`, `METR`).
- **Opciones** (`GFGC...`, `GFGV...`): sus precios varían hasta 50 veces en el trimestre de forma legítima, por su volatilidad.
- **Montos extremos legítimos:** un único cliente opera letras por más de 1.000M de nominales. Las letras y bonos cotizan por valor nominal (precio ≈ 1), así que un umbral global de cantidad o precio no sirve.

**Decisión.**
- `PRECIO_FUERA_DE_RANGO`: el desvío simétrico `max(p/mediana, mediana/p)` es mayor a **N = 10**, contra la mediana del símbolo y moneda en todo el histórico (`precio_referencia`). Se exigen al menos 5 operaciones del símbolo y no se aplica a opciones.
- **Calibración de N:** sin opciones, los 8 casos con desvío mayor a 100x son todos placeholders, y los movimientos reales no superan 3,6x. **No hay ningún caso entre 3,6x y 100x**, así que N = 10 cae en el medio de ese hueco natural. Marca 8 filas.
- `MONTO_EXTREMO`: monto por encima del **p99.9 de su tipo de instrumento**. Marca 104 filas. Es informativo, porque son operaciones legítimas que distorsionan promedios.
- Ninguna fila se descarta. Gold decide qué filas usa según `dq_flags`.

**Alternativas consideradas.**
- *Umbral global de precio o cantidad:* no sirve, por las distintas escalas entre letras, bonos y acciones.
- *Z-score o IQR:* asumen una distribución que los precios de un trimestre no cumplen, y son sensibles a los mismos outliers que buscan detectar. La mediana es robusta.

---

## DT-007: Segmentación de clientes ante distribución asimétrica

- **Estado:** Aceptada
- **Fecha:** 2026-10-15

**Contexto (perfilado).**
- 57.655 clientes. La mediana es 1 operación por cliente y el máximo es 676. El 1% más activo concentra el 12,4% de las operaciones.
- **Los percentiles no discriminan:** el p75 de operaciones por cliente es 1. Cortar por cuartiles pondría al 75% de los clientes en el mismo grupo.
- El 82,7% de los clientes operó en un solo mes. Marzo está incompleto (el dataset termina el 13/03).
- **El dataset no trae atributos de cliente**, solo `id_cliente`.

**Decisión.**
- Los atributos de `dim_cliente` se **derivan del comportamiento**: `segmento_actividad` y `canal_principal`. En producción, la dimensión combinaría atributos del maestro de clientes (CRM: perfil de inversor, fecha de alta) con estos atributos derivados; el mecanismo de SCD2 (DT-016) es el mismo.
- `segmento_actividad` usa **umbrales absolutos sobre operaciones acumuladas**: 1 → `OCASIONAL`, 2-4 → `RECURRENTE`, 5-19 → `FRECUENTE`, 20+ → `INTENSIVO`. El último umbral está cerca del p99.9. Distribución al último corte: 40.841 / 14.387 / 2.257 / 170.
- `canal_principal`: el canal con más operaciones acumuladas; en caso de empate, el más reciente.

**Alternativas consideradas.**
- *Inventar un maestro de clientes:* serían datos fabricados, y cualquier análisis sobre ellos carecería de significado.
- *Percentiles:* no discriminan por la asimetría.
- *Segmentar por volumen operado:* mezcla ARS y USD y requeriría un tipo de cambio. Se puede sumar cuando estén las cotizaciones (Task 04).
- *Actividad del mes, en lugar de acumulada:* con el 82,7% de clientes activos un solo mes, el segmento oscilaría entre "activo" e "inactivo" todos los meses y perdería valor descriptivo.

**Consecuencias.** Con actividad acumulada, el segmento solo puede subir. Es razonable para un trimestre; en una historia larga convendría una ventana móvil (por ejemplo, 12 meses) para reflejar clientes que dejan de operar.

---

## DT-008: Schema `landing` propio para archivos crudos

- **Estado:** Aceptada
- **Fecha:** 2026-10-15

**Contexto.** Hace falta un lugar para los archivos fuente antes de la capa Bronze. En Databricks Free Edition, DBFS root y `/FileStore` están deprecados, y lo recomendado son los volúmenes de Unity Catalog, que siempre viven dentro de un schema.

**Decisión.** Un schema `workspace.landing` con el volumen `raw_files` y una carpeta por fuente: `/Volumes/workspace/landing/raw_files/transacciones/`. Los checkpoints de Auto Loader van en un volumen aparte: `workspace.bronze.checkpoints`.

**Alternativas consideradas.**
- *Volumen dentro del schema `bronze`:* tiene menos piezas, pero mezcla archivos y tablas y los permisos quedan compartidos.

**Consecuencias.** La arquitectura queda como *landing (archivos) → bronze → silver → gold*. Landing emula una *external location* (S3/ADLS) donde el sistema fuente depositaría un archivo por día; en producción solo cambia la ruta en `config.py`.

---

## DT-009: Notebooks como disparadores, lógica en `src/`

- **Estado:** Aceptada
- **Fecha:** 2026-10-15

**Contexto.** La consigna pide notebooks con outputs visibles, y el bonus pide unit tests que corran sin cluster.

**Decisión.**
- La lógica vive en el paquete Python `src/iol_pipeline/`. Los notebooks (`notebooks/`) solo leen parámetros, llaman a esas funciones y muestran validaciones.
- `config.py` centraliza catálogo, schemas y rutas.
- PySpark se importa dentro de las funciones que lo usan, así los tests (`tests/`, `pytest`) corren sin Spark.
- Un test verifica que el schema de Auto Loader coincida con el DDL de `setup/02_bronze.sql`, para que no diverjan.
- Los notebooks se versionan directamente en `.ipynb`, con outputs: la consigna exige outputs visibles y GitHub los muestra renderizados. Se descartó el formato fuente de Databricks (`.py`), que tiene diffs limpios pero no guarda outputs y obligaría a mantener una copia exportada. El costo de los diffs ruidosos es bajo porque la lógica que más cambia vive en `src/`.

**Alternativas consideradas.**
- *Toda la lógica en notebooks:* es más rápido al inicio, pero no se puede testear sin cluster y la lógica se duplica entre notebooks.

**Consecuencias.** Los notebooks agregan `src/` al `sys.path`, lo que asume la estructura de carpetas del repo (Git folder en Databricks).

---

## DT-010: Orquestación con notebook en lugar de Databricks Jobs

- **Estado:** Aceptada
- **Fecha:** 2026-10-15

**Contexto.** Databricks Free Edition incluye Jobs (DAG de tareas, schedule, reintentos, alertas), pero la consigna los prohíbe explícitamente y permite simular la orquestación con `%run` o `dbutils.notebook.run()`.

**Decisión.** `notebooks/00_orquestador` ejecuta los pasos en orden con `dbutils.notebook.run()`. Cada notebook devuelve un JSON con su `status` y el orquestador se detiene ante el primer `FAIL`, marca el resto como `SKIPPED` y muestra un resumen con status y duración por paso. Se elige `dbutils.notebook.run()` sobre `%run` porque aísla el contexto de cada paso y permite capturar su resultado.

**Alternativas consideradas.**
- *Databricks Jobs:* es la opción productiva, pero la consigna lo prohíbe. Se documentará como diseño productivo en la arquitectura.
- *`%run`:* comparte variables entre notebooks (acoplamiento) y no devuelve un resultado.

**Consecuencias.** No hay schedule, reintentos automáticos ni paralelismo entre pasos; la ejecución es manual.

---

## DT-011: Clasificación de instrumentos por reglas

- **Estado:** Aceptada
- **Fecha:** 2026-10-15

**Contexto.** El dataset no trae el tipo de instrumento. La descripción lo sugiere ("Cedear ...", "Bono ...", "Letra ..."), pero hay variaciones de mayúsculas, typos ("CEDEARV") y descripciones que son el propio ticker (opciones como `GFGC79054F`).

**Decisión.** Reglas evaluadas en orden (`INSTRUMENT_RULES` en `silver.py`), con regex sobre el símbolo o sobre la descripción normalizada. La primera que matchea define el tipo: `OPCION`, `CEDEAR`, `BONO`, `ON`, `LETRA`, `WARRANT`. Después, palabras clave para deuda sin prefijo (`CLASE`/`REGS` → ON; `VTO`, `%`, `TITULOS DE DEUDA`, `CUPÓN` → BONO). Lo que no matchea se clasifica como `ACCION`. Las reglas están declaradas como datos: la versión Python (tests) y la expresión Spark se generan de la misma lista.

**Resultado:** CEDEAR 54,0% · BONO 19,0% · ACCION 18,2% · LETRA 4,1% · ON 2,6% · OPCION 2,2% · WARRANT 0,1%.

**Alternativas consideradas.**
- *Tabla de mapeo manual por símbolo:* es más precisa, pero con 1.445 símbolos no es mantenible.
- *Fuente externa de metadatos:* se evaluará junto con la API de cotizaciones (Task 04).

**Consecuencias.** `ACCION` es una categoría por descarte: puede contener instrumentos de deuda con descripciones atípicas. Es el candidato natural para la integración con IA (Task 07).

---

## DT-012: Silver se reconstruye completo en cada corrida

- **Estado:** Aceptada
- **Fecha:** 2026-10-15

**Contexto.** Las reglas de calidad usan referencias sobre todo el histórico: la mediana de precio por símbolo y el p99.9 de monto por tipo de instrumento. Con 100.000 filas, procesar todo tarda segundos.

**Decisión.** Cada corrida recalcula Silver completo desde Bronze y sobrescribe la tabla con `writeTo(...).overwrite(...)`, que conserva el DDL, los comentarios y las propiedades. Es idempotente por diseño: el mismo Bronze produce el mismo Silver. El notebook valida el balance `bronze = silver + duplicados` y que no haya fechas sin calendario.

**Alternativas consideradas.**
- *Incremental con `MERGE` por `id_transaccion`:* escala mejor, pero las medianas y percentiles incrementales requieren mantener estado aparte y recalcular flags de filas viejas cuando cambia la referencia. Es complejidad que hoy no se justifica.

**Estrategia de escalabilidad.** Si el volumen crece (por ejemplo, años de historia con archivos diarios), Silver pasaría a ser incremental: leer de Bronze solo lo nuevo con Change Data Feed (ya habilitado), aplicar `MERGE` por `id_transaccion`, y recalcular las referencias estadísticas en una tabla aparte, con una ventana móvil, por ejemplo de 90 días.

---

## DT-013: Outputs públicos sin datos a nivel registro

- **Estado:** Aceptada
- **Fecha:** 2026-10-15

**Contexto.** Los notebooks se publican con outputs en un repo público, y la consigna prohíbe subir los datos originales.

**Decisión.** Los outputs de los notebooks muestran **agregados** (conteos, porcentajes, detalle por símbolo), nunca filas con `id_cliente` o `id_transaccion`. Antes de cada commit con outputs se verifica que no aparezcan identificadores con los patrones `CLI…` y `TXN…`.

**Consecuencias.** Para inspeccionar registros individuales se consulta la tabla directamente en Databricks, fuera de los notebooks versionados.

---

## DT-014: Registro declarativo de reglas de calidad

- **Estado:** Aceptada
- **Fecha:** 2026-10-15

**Contexto.** Las reglas de calidad de Silver estaban escritas a mano dentro de la transformación: agregar una regla implicaba tocar la lógica en varios lugares, y no había forma de listar qué se valida sin leer el código.

**Decisión.** Las reglas se declaran como datos en `DQ_RULES` (`src/iol_pipeline/silver.py`). Cada `DQRule` es inmutable (`frozen=True`) y tiene `name`, `description`, `condition` (expresión SQL: TRUE = la fila tiene el problema) y `severity` (`WARN` o `ERROR`). `apply_dq_rules()` genera `dq_flags` y `dq_status` a partir de la lista:
- Una condición que evalúa a `NULL` no marca la fila: solo se marca lo que se puede afirmar.
- `dq_status` es la peor severidad: `OK` sin flags, `WARN`, o `ERROR` si algún flag es de severidad `ERROR`.
- Los umbrales (`PRICE_DEVIATION_THRESHOLD`, etc.) se interpolan en las condiciones, así no hay números duplicados.
- El notebook publica el catálogo (regla, descripción, severidad, condición, filas marcadas), así se puede saber qué se valida sin leer el código (Task 05).

Hoy las tres reglas son `WARN`. `ERROR` queda disponible para controles que deban excluir filas de Gold.

**Alternativas consideradas.**
- *Condiciones como expresiones `Column` de PySpark:* son más seguras en tipos, pero no se pueden mostrar como texto ni persistir en una tabla.
- *Framework externo (Great Expectations, DQX):* agrega una dependencia para tres reglas. Delta Live Tables, que tiene *expectations* nativas, está prohibido por la consigna.

**Consecuencias.** Las condiciones son strings SQL, y los tests sin Spark solo validan su estructura (nombres únicos, severidad válida, umbrales reflejados). Su ejecución se testea con PySpark local, en tests que se saltean si PySpark no está instalado.

---

## DT-015: Modelo en estrella con claves subrogadas deterministas

- **Estado:** Aceptada
- **Fecha:** 2026-10-15

**Contexto.** Gold tiene que responder consultas analíticas de negocio (Task 06): comportamiento por cliente, instrumento, canal y tiempo.

**Decisión.** Esquema en **estrella**:
- `fact_operaciones`, con grano de **una transacción**. Contiene las claves de las dimensiones, medidas (`cantidad`, `precio`, `monto`), `id_transaccion` como dimensión degenerada y los flags de calidad de Silver. Conserva todas las filas: el análisis filtra por `dq_status` según lo que necesite.
- `dim_cliente` (SCD2, DT-016), `dim_instrumento` (un símbolo, con la descripción más frecuente como canónica), `dim_canal` (con tipo APP / WEB / API / OTRO) y `dim_fecha` (días hábiles y feriados; la clave del hecho es la `fecha_operativa`).
- **Claves subrogadas deterministas:** `xxhash64` de la clave natural, y en `dim_cliente` de `(id_cliente, valid_from)`. Como Gold se reconstruye completo (DT-012), las claves no cambian entre corridas, así que una consulta guardada o un dashboard no se rompen.
- Controles de integridad en cada corrida: el hecho conserva todas las filas de Silver, no hay claves huérfanas y el SCD2 cumple sus invariantes.

**Alternativas consideradas.**
- *Copo de nieve* (por ejemplo, separar tipo de instrumento en su propia tabla): normaliza dimensiones chicas a cambio de más joins en cada consulta.
- *Tabla plana desnormalizada (one big table):* es simple de consultar, pero repite los atributos del cliente en cada operación y complica el SCD2.
- *Claves autoincrementales (`IDENTITY`):* cambian si la tabla se reconstruye.

**Consecuencias.** La Task 04 agregará `precio_mercado` y el desvío al hecho como columnas nuevas.

---

## DT-016: SCD Tipo 2 en `dim_cliente` con cortes mensuales

- **Estado:** Aceptada
- **Fecha:** 2026-10-15

**Contexto.** La consigna pide que la dimensión de clientes soporte cambios de atributos en el tiempo, y demostrarlo con un caso concreto.

**Decisión.**
- **Atributos versionados:** `segmento_actividad` y `canal_principal` (DT-007). `fecha_primera_operacion` no se versiona porque no cambia.
- **Cargas:** un corte por mes (31/01, 28/02 y 13/03), aplicados en orden como cargas sucesivas. Cada corte calcula los atributos con la actividad acumulada hasta esa fecha, según la `fecha_operativa`.
- **Mecanismo:** `MERGE` de Delta con el patrón de *staging*. Un hash de los atributos (`_attr_hash`) detecta cambios. Si cambian, se cierra la versión vigente (`valid_to` = día anterior, `is_current = false`) y se inserta una nueva. Si no cambian, no se toca nada, así que re-aplicar un corte es idempotente.
- **Vigencia:** una versión rige desde el inicio del mes en que se calculó (`valid_from`). Así, el segmento de un cliente en enero es el que tenía al cierre de enero.
- **Point-in-time join:** el hecho se une con la versión vigente en la `fecha_operativa` de cada operación.
- Como Gold se reconstruye completo (DT-012), la dimensión se vacía y se re-aplican todos los cortes. En producción, cada mes se aplicaría solo el corte nuevo con la misma función (`apply_scd2`).

**Resultado.** 65.471 versiones para 57.655 clientes. Cambios: 4.626 en febrero y 3.190 en marzo. 192 clientes tienen 3 versiones. El notebook muestra uno de ellos (enmascarado, DT-013) y sus operaciones asociadas a cada versión.

**Alternativas consideradas.**
- *SCD Tipo 1 (sobrescribir):* pierde la historia; una operación de enero quedaría con el segmento de marzo.
- *Calcular la historia completa con funciones de ventana*, sin `MERGE`: es más simple para una reconstrucción completa, pero no muestra el mecanismo incremental que se usaría en producción.
- *Cortes diarios:* generarían versiones por cambios intradiarios de poco valor analítico y una dimensión mucho más grande.

**Consecuencias.** La vigencia desde el inicio del mes implica que el segmento asignado a una operación usa la actividad del mes completo, incluida la posterior a esa operación. Es una convención de "segmento del período"; si se necesitara el segmento exacto al momento de cada operación, harían falta cortes diarios.

---

## Plantilla para nuevas decisiones

```markdown
## DT-XXX: Título

- **Estado:** Propuesta
- **Fecha:** AAAA-MM-DD

**Contexto.** Qué problema o situación motiva la decisión (con datos si los hay).

**Decisión.** Qué se decidió.

**Alternativas consideradas.** Qué otras opciones hubo y por qué se descartaron.

**Consecuencias.** Tradeoffs, deuda técnica, impacto en otras capas.
```
