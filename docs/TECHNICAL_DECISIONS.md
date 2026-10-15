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
| [DT-004](#dt-004-tratamiento-de-operaciones-en-días-inhábiles) | Tratamiento de operaciones en días inhábiles | Calidad / Silver | Propuesta |
| [DT-005](#dt-005-identificación-de-variantes-de-liquidación-mep--cable) | Identificación de variantes de liquidación (MEP / Cable) | Calidad / Enriquecimiento | Propuesta |
| [DT-006](#dt-006-detección-de-montos-y-precios-extremos) | Detección de montos y precios extremos | Calidad | Propuesta |
| [DT-007](#dt-007-segmentación-de-clientes-ante-distribución-asimétrica) | Segmentación de clientes ante distribución asimétrica | Gold | Propuesta |
| [DT-008](#dt-008-schema-landing-propio-para-archivos-crudos) | Schema `landing` propio para archivos crudos | Arquitectura | Aceptada |
| [DT-009](#dt-009-notebooks-como-disparadores-lógica-en-src) | Notebooks como disparadores, lógica en `src/` | Arquitectura / Testing | Aceptada |
| [DT-010](#dt-010-orquestación-con-notebook-en-lugar-de-databricks-jobs) | Orquestación con notebook en lugar de Databricks Jobs | Orquestación | Aceptada |

## Pendientes

- [ ] Escribir el `README.md`: diagrama de arquitectura, instrucciones de reproducción, decisiones con tradeoffs (enlazando este documento), tabla de cobertura de cotizaciones por símbolo y documentación del componente de IA.
- [x] Definir el mecanismo de idempotencia de la ingesta a Bronze: Auto Loader (DT-003).
- [x] Definir la ruta del volumen de Unity Catalog donde se aloja el CSV (DT-008).
- [ ] Calibrar el umbral N de desvío de precio contra la mediana (DT-006).
- [ ] Evaluar si se versiona una muestra ofuscada del dataset (DT-001).
- [ ] Activar "Commit notebook outputs" en el Git folder de Databricks.
- [ ] Ejecutar setup y notebooks en Databricks y commitear los `.ipynb` con outputs.
- [ ] Documentar en arquitectura el Job productivo equivalente al orquestador (DT-010).

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

- **Estado:** Propuesta
- **Fecha:** 2026-10-15

**Contexto (perfilado).**
- 12.498 operaciones (12,5%) caen en **sábado**, todos los sábados del período. No hay operaciones en domingo ni en los feriados (01/01, 16/02 y 17/02).
- Las horas UTC cubren las 24 horas del día, aunque el horario de BYMA es de 11 a 17 ART (14 a 20 UTC). Los sábados la actividad va de 00 a 19 UTC y los lunes empieza a las 10 UTC.
- Interpretación: son órdenes cargadas fuera del horario de mercado por canales 24/7, que se ejecutan en la siguiente rueda.

**Decisión propuesta.** Conservar el `fecha` original y derivar `fecha_operativa`, que corresponde al siguiente día hábil según un calendario de feriados argentinos (`dim_calendario`), junto con el flag `es_dia_inhabil`. Para el enriquecimiento con cotizaciones se usa `fecha_operativa`.

**Alternativas consideradas.**
- *Descartar operaciones de sábado:* viola el requisito de trazabilidad.
- *Usar el último cierre anterior:* para una orden cargada fuera de horario, el precio relevante es el de la próxima rueda.

---

## DT-005: Identificación de variantes de liquidación (MEP / Cable)

- **Estado:** Propuesta
- **Fecha:** 2026-10-15

**Contexto (perfilado).**
- El 100% de las operaciones en USD usa símbolos con sufijo `D` (MEP) o `C` (Cable), como `AL30D`/`AL30C` o `AAPLD`/`AAPLC`.
- Hay **falsos positivos**: 4.648 operaciones en ARS con tickers reales que terminan en D o C (`YPFD`, `GLD`, `AMD`, `INTC`, `LAC`, `JD`, `MCD`, `HOOD`).
- Hay casos irregulares, como `BA.C`, `BA.CD` y `BA7DC`.

**Decisión propuesta.** Una regla compuesta: se considera variante solo si `moneda = 'USD'`, el símbolo termina en `D` o `C` y el ticker base existe en el dataset. Los casos que no resuelve la regla se cargan en una tabla de mapeo explícita (`ref_simbolo_variante`). Se obtienen `simbolo_base` y `tipo_liquidacion` (`ARS`, `MEP`, `CABLE`).

**Alternativas consideradas.**
- *Solo por sufijo:* clasificaría mal tickers como YPFD o GLD.
- *Solo por moneda:* no distingue MEP de Cable.

---

## DT-006: Detección de montos y precios extremos

- **Estado:** Propuesta
- **Fecha:** 2026-10-15

**Contexto (perfilado).**
- **Precios placeholder:** valores de `0.01` o cercanos a `1.00` en instrumentos con medianas muy superiores (`AL30` tiene mediana de 876 y mínimo de 0.01; también `SATL`, `TGNO4`, `METR`).
- **Montos extremos legítimos:** el cliente `CLI74090760` opera letras por más de 1.000M de nominales (hasta 1.470M de ARS). Las letras y bonos cotizan por valor nominal (precio ≈ 1), así que un umbral global de cantidad o precio no sirve.

**Decisión propuesta.** Reglas **relativas por `simbolo_titulo` + `moneda`** y no globales: el flag `precio_fuera_de_rango` se activa si el precio se desvía más de N veces de la mediana del símbolo (N a calibrar). Los montos extremos se marcan con `monto_extremo` por percentil dentro del tipo de instrumento, sin excluirse. Todos los flags se aplican en Silver (DT-003).

---

## DT-007: Segmentación de clientes ante distribución asimétrica

- **Estado:** Propuesta
- **Fecha:** 2026-10-15

**Contexto (perfilado).** 57.655 clientes. La mediana es 1 operación por cliente y el máximo es 676. El 1% más activo concentra el 12,4% de las operaciones. Los promedios no son representativos.

**Decisión propuesta.** Segmentar por percentiles de actividad o volumen (por ejemplo: ocasional, recurrente, heavy y top/institucional) como atributo de `dim_cliente` con SCD Tipo 2, para que un cliente pueda cambiar de segmento a lo largo del tiempo. Este cambio es el caso concreto que pide la consigna para demostrar el SCD.

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
