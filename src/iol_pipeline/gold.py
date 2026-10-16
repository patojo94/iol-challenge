"""Capa Gold: modelo dimensional en estrella (DT-015).

    dim_fecha ─┐
    dim_canal ─┼── fact_operaciones ──┬─ dim_instrumento
               │                      └─ dim_cliente (SCD Tipo 2)

- Grano del hecho: una fila por transacción de Silver.
- Claves subrogadas deterministas (`xxhash64` de la clave natural): el mismo
  Silver produce las mismas claves, así la reconstrucción es idempotente.
- `dim_cliente` versiona sus atributos con SCD Tipo 2 (DT-016) y el hecho se
  une con la versión vigente en la `fecha_operativa` de cada operación.

La lógica pura (segmentación, cortes, calendario, evaluación) no depende de
Spark y se testea sin cluster.
"""

import calendar
from dataclasses import asdict, dataclass
from datetime import date, timedelta

from iol_pipeline.silver import FERIADOS_AR, is_business_day

# --- Segmentación de clientes (DT-007) ---------------------------------------------

# Umbrales absolutos sobre operaciones acumuladas, de mayor a menor. Los
# percentiles no discriminan: el p75 de operaciones por cliente es 1.
SEGMENT_THRESHOLDS: list[tuple[int, str]] = [
    (20, "INTENSIVO"),
    (5, "FRECUENTE"),
    (2, "RECURRENTE"),
    (1, "OCASIONAL"),
]


def activity_segment(operations: int) -> str:
    for minimum, segment in SEGMENT_THRESHOLDS:
        if operations >= minimum:
            return segment
    raise ValueError(f"Un cliente en la dimensión tiene al menos 1 operación: {operations}")


# --- Cortes de carga del SCD2 (DT-016) ----------------------------------------------


def monthly_cutoffs(start: date, end: date) -> list[tuple[date, date]]:
    """Cortes mensuales entre start y end: (inicio_del_período, fecha_de_corte).

    El último corte es `end` si el mes está incompleto (marzo termina el 13).
    """
    cutoffs = []
    period_start = start.replace(day=1)
    while period_start <= end:
        last_day = calendar.monthrange(period_start.year, period_start.month)[1]
        cutoffs.append((period_start, min(period_start.replace(day=last_day), end)))
        period_start = period_start.replace(day=last_day) + timedelta(days=1)
    return cutoffs


SCD_TRACKED_ATTRIBUTES = ("segmento_actividad", "canal_principal")
SCD_END_DATE = date(9999, 12, 31)


def mask_client_id(id_cliente: str) -> str:
    """Enmascara un id para mostrarlo en outputs públicos (DT-013): CLI***0TEST."""
    return f"{id_cliente[:3]}***{id_cliente[-5:]}" if len(id_cliente) > 8 else "***"


# --- Dimensiones simples -------------------------------------------------------------

CANAL_TYPES = {
    "App Mobile": "APP",
    "Sitio Web Desktop": "WEB",
    "Sitio Web Responsive": "WEB",
    "API": "API",
}
DEFAULT_CANAL_TYPE = "OTRO"  # IOLnet y cualquier canal nuevo

MONTH_NAMES = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio",
               "agosto", "septiembre", "octubre", "noviembre", "diciembre"]
DAY_NAMES = ["lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo"]


def date_key(day: date) -> int:
    return day.year * 10000 + day.month * 100 + day.day


def date_dimension_rows(start: date, end: date, holidays: dict[date, str] = FERIADOS_AR) -> list[tuple]:
    rows = []
    for i in range((end - start).days + 1):
        d = start + timedelta(days=i)
        rows.append((
            date_key(d), d, d.year, d.month, MONTH_NAMES[d.month - 1], (d.month - 1) // 3 + 1,
            d.day, d.isoweekday(), DAY_NAMES[d.weekday()], d.weekday() >= 5,
            d in holidays, holidays.get(d), is_business_day(d, set(holidays)),
        ))
    return rows


# --- Schemas de salida ---------------------------------------------------------------
# Deben coincidir con setup/05_gold.sql (lo verifica tests/test_gold.py).

DIM_FECHA_COLUMNS = [
    "fecha_key", "fecha", "anio", "mes", "nombre_mes", "trimestre", "dia", "dia_semana",
    "nombre_dia", "es_fin_de_semana", "es_feriado", "nombre_feriado", "es_dia_habil",
]
DIM_CANAL_COLUMNS = ["canal_key", "canal", "tipo_canal"]
DIM_INSTRUMENTO_COLUMNS = [
    "instrumento_key", "simbolo", "simbolo_base", "tipo_instrumento", "tipo_liquidacion",
    "moneda", "descripcion", "descripciones_distintas", "fecha_primera_operacion",
    "fecha_ultima_operacion",
]
DIM_CLIENTE_COLUMNS = [
    "cliente_sk", "id_cliente", "segmento_actividad", "canal_principal",
    "fecha_primera_operacion", "valid_from", "valid_to", "is_current", "_attr_hash", "_loaded_at",
]
FACT_COLUMNS = [
    "id_transaccion", "fecha_key", "cliente_sk", "instrumento_key", "canal_key",
    "fecha_hora_local", "tipo_operacion", "moneda", "cantidad", "precio", "monto",
    "en_horario_mercado", "dq_status", "dq_flags", "_processed_at",
]


# --- Evaluación de la corrida -----------------------------------------------------------

STATUS_OK = "OK"
STATUS_FAIL = "FAIL"


@dataclass
class GoldResult:
    silver_rows: int
    fact_rows: int
    orphan_keys: dict
    clients_without_single_current: int
    overlapping_versions: int
    status: str
    message: str

    def to_dict(self) -> dict:
        return asdict(self)


def evaluate_gold(
    silver_rows: int,
    fact_rows: int,
    orphan_keys: dict,
    clients_without_single_current: int,
    overlapping_versions: int,
) -> GoldResult:
    """Integridad del modelo: el hecho conserva todas las filas, toda clave
    resuelve a su dimensión y el SCD2 cumple sus invariantes."""
    problems = []
    if fact_rows != silver_rows:
        problems.append(f"fact tiene {fact_rows} filas y silver {silver_rows}")
    problems += [f"{n} filas sin {key}" for key, n in orphan_keys.items() if n]
    if clients_without_single_current:
        problems.append(f"{clients_without_single_current} clientes sin exactamente una versión vigente")
    if overlapping_versions:
        problems.append(f"{overlapping_versions} pares de versiones con vigencias superpuestas")

    status = STATUS_FAIL if problems else STATUS_OK
    message = "; ".join(problems) if problems else f"{fact_rows} operaciones modeladas, integridad referencial y SCD2 OK"
    return GoldResult(silver_rows, fact_rows, orphan_keys, clients_without_single_current,
                      overlapping_versions, status, message)


# --- Spark ---------------------------------------------------------------------------------
# PySpark se importa dentro de las funciones para que los tests corran sin Spark.


def _segment_expr(col_name: str):
    from pyspark.sql import functions as F

    expr = None
    for minimum, segment in SEGMENT_THRESHOLDS:
        cond = F.col(col_name) >= minimum
        expr = F.when(cond, segment) if expr is None else expr.when(cond, segment)
    return expr


def build_dim_fecha(spark, start: date, end: date):
    schema = (
        "fecha_key INT, fecha DATE, anio INT, mes INT, nombre_mes STRING, trimestre INT, dia INT, "
        "dia_semana INT, nombre_dia STRING, es_fin_de_semana BOOLEAN, es_feriado BOOLEAN, "
        "nombre_feriado STRING, es_dia_habil BOOLEAN"
    )
    return spark.createDataFrame(date_dimension_rows(start, end), schema)


def build_dim_canal(silver_df):
    from pyspark.sql import functions as F

    tipo = None
    for canal, canal_type in CANAL_TYPES.items():
        cond = F.col("canal") == canal
        tipo = F.when(cond, canal_type) if tipo is None else tipo.when(cond, canal_type)
    return (
        silver_df.select("canal").distinct()
        .withColumn("canal_key", F.xxhash64("canal"))
        .withColumn("tipo_canal", tipo.otherwise(DEFAULT_CANAL_TYPE))
        .select(*DIM_CANAL_COLUMNS)
    )


def build_dim_instrumento(silver_df):
    """Un registro por símbolo. Si un símbolo tiene varias descripciones (ej. AZN),
    se toma la más frecuente como canónica, junto con su tipo de instrumento."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    by_description = silver_df.groupBy(
        "simbolo", "simbolo_base", "tipo_liquidacion", "moneda", "descripcion_titulo", "tipo_instrumento"
    ).agg(F.count("*").alias("_n"))
    rank = Window.partitionBy("simbolo").orderBy(F.desc("_n"), "descripcion_titulo")
    canonical = (
        by_description.withColumn("_rank", F.row_number().over(rank)).where("_rank = 1")
        .withColumnRenamed("descripcion_titulo", "descripcion")
    )
    stats = silver_df.groupBy("simbolo").agg(
        F.countDistinct("descripcion_titulo").cast("int").alias("descripciones_distintas"),
        F.min("fecha_operativa").alias("fecha_primera_operacion"),
        F.max("fecha_operativa").alias("fecha_ultima_operacion"),
    )
    return (
        canonical.join(stats, "simbolo")
        .withColumn("instrumento_key", F.xxhash64("simbolo"))
        .select(*DIM_INSTRUMENTO_COLUMNS)
    )


def client_snapshot(silver_df, cutoff: date):
    """Atributos de cada cliente con su actividad acumulada hasta el corte."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    activity = silver_df.where(F.col("fecha_operativa") <= F.lit(cutoff))
    totals = activity.groupBy("id_cliente").agg(
        F.count("*").alias("operaciones"),
        F.min("fecha_operativa").alias("fecha_primera_operacion"),
    )
    # Canal principal: el más usado; si empatan, el más reciente y luego el nombre.
    by_canal = activity.groupBy("id_cliente", "canal").agg(
        F.count("*").alias("_n"), F.max("fecha_hora_utc").alias("_ultima")
    )
    rank = Window.partitionBy("id_cliente").orderBy(F.desc("_n"), F.desc("_ultima"), "canal")
    main_canal = (
        by_canal.withColumn("_rank", F.row_number().over(rank)).where("_rank = 1")
        .select("id_cliente", F.col("canal").alias("canal_principal"))
    )
    return (
        totals.join(main_canal, "id_cliente")
        .withColumn("segmento_actividad", _segment_expr("operaciones"))
        .withColumn("_attr_hash", F.sha2(F.concat_ws("||", *SCD_TRACKED_ATTRIBUTES), 256))
        .select("id_cliente", *SCD_TRACKED_ATTRIBUTES, "fecha_primera_operacion", "_attr_hash")
    )


def apply_scd2(spark, dim_table: str, snapshot_df, effective_date: date, loaded_at) -> dict:
    """Aplica un snapshot a la dimensión con SCD Tipo 2 (MERGE de Delta).

    - Cliente nuevo: se inserta su primera versión.
    - Atributos distintos a la versión vigente: se cierra la vigente
      (valid_to = día anterior a effective_date) y se inserta una nueva.
    - Sin cambios: no se toca nada (re-aplicar el mismo snapshot es idempotente).

    Patrón de "staging": cada cliente cambiado aparece dos veces en el origen
    del MERGE, una con merge_key = id (cierra la versión vigente) y otra con
    merge_key = NULL (nunca matchea, así que inserta la nueva versión).
    """
    from pyspark.sql import functions as F

    current = spark.table(dim_table).where("is_current").select(
        "id_cliente", F.col("_attr_hash").alias("_current_hash")
    )
    compared = snapshot_df.join(current, "id_cliente", "left")
    changed = compared.where("_current_hash IS NOT NULL AND _current_hash <> _attr_hash").drop("_current_hash")
    stats = compared.agg(
        F.sum(F.col("_current_hash").isNull().cast("int")).alias("nuevos"),
        F.sum((F.col("_current_hash") != F.col("_attr_hash")).cast("int")).alias("con_cambios"),
        F.sum((F.col("_current_hash") == F.col("_attr_hash")).cast("int")).alias("sin_cambios"),
    ).first()

    staged = snapshot_df.withColumn("_merge_key", F.col("id_cliente")).unionByName(
        changed.withColumn("_merge_key", F.lit(None).cast("string"))
    )
    view = "_scd2_staged"
    staged.createOrReplaceTempView(view)
    spark.sql(f"""
        MERGE INTO {dim_table} AS t
        USING {view} AS s
        ON t.id_cliente = s._merge_key AND t.is_current
        WHEN MATCHED AND t._attr_hash <> s._attr_hash THEN UPDATE SET
            t.is_current = false,
            t.valid_to   = date_sub(DATE'{effective_date}', 1)
        WHEN NOT MATCHED THEN INSERT (
            cliente_sk, id_cliente, segmento_actividad, canal_principal, fecha_primera_operacion,
            valid_from, valid_to, is_current, _attr_hash, _loaded_at
        ) VALUES (
            xxhash64(s.id_cliente, DATE'{effective_date}'), s.id_cliente, s.segmento_actividad,
            s.canal_principal, s.fecha_primera_operacion, DATE'{effective_date}', DATE'{SCD_END_DATE}',
            true, s._attr_hash, TIMESTAMP'{loaded_at}'
        )
    """)
    return {"fecha_efectiva": effective_date, **{k: stats[k] or 0 for k in ("nuevos", "con_cambios", "sin_cambios")}}


def build_dim_cliente(spark, silver_df, dim_table: str, cutoffs: list[tuple[date, date]], loaded_at) -> list[dict]:
    """Reconstruye la dimensión desde cero aplicando los cortes en orden,
    como si fueran cargas mensuales sucesivas (DT-016)."""
    spark.sql(f"DELETE FROM {dim_table}")
    loads = []
    for period_start, cutoff in cutoffs:
        snapshot = client_snapshot(silver_df, cutoff)
        loads.append({"corte": cutoff, **apply_scd2(spark, dim_table, snapshot, period_start, loaded_at)})
    return loads


def build_fact(silver_df, dim_cliente_df, processed_at):
    """Hecho con grano transacción. El cliente se une con la versión vigente
    en la fecha operativa (point-in-time join)."""
    from pyspark.sql import functions as F

    clientes = dim_cliente_df.select("cliente_sk", "id_cliente", "valid_from", "valid_to")
    s = silver_df.alias("s")
    c = clientes.alias("c")
    joined = s.join(
        c,
        (F.col("s.id_cliente") == F.col("c.id_cliente"))
        & F.col("s.fecha_operativa").between(F.col("c.valid_from"), F.col("c.valid_to")),
        "left",
    )
    return joined.select(
        F.col("s.id_transaccion").alias("id_transaccion"),
        F.date_format("s.fecha_operativa", "yyyyMMdd").cast("int").alias("fecha_key"),
        F.col("c.cliente_sk").alias("cliente_sk"),
        F.xxhash64("s.simbolo").alias("instrumento_key"),
        F.xxhash64("s.canal").alias("canal_key"),
        F.col("s.fecha_hora_local").alias("fecha_hora_local"),
        F.col("s.tipo_operacion").alias("tipo_operacion"),
        F.col("s.moneda").alias("moneda"),
        F.col("s.cantidad").alias("cantidad"),
        F.col("s.precio").alias("precio"),
        F.col("s.monto").alias("monto"),
        F.col("s.en_horario_mercado").alias("en_horario_mercado"),
        F.col("s.dq_status").alias("dq_status"),
        F.col("s.dq_flags").alias("dq_flags"),
        F.lit(processed_at).cast("timestamp").alias("_processed_at"),
    )


def validate_gold(spark, tables: dict, silver_rows: int) -> GoldResult:
    """Controles de integridad sobre las tablas escritas.

    `tables` mapea: fact, dim_cliente, dim_instrumento, dim_canal, dim_fecha.
    """
    fact = spark.table(tables["fact"])
    orphans = {}
    for key, dim in [("fecha_key", "dim_fecha"), ("instrumento_key", "dim_instrumento"),
                     ("canal_key", "dim_canal"), ("cliente_sk", "dim_cliente")]:
        dim_keys = spark.table(tables[dim]).select(key).distinct()
        orphans[key] = fact.select(key).join(dim_keys, key, "left_anti").count()

    dim = tables["dim_cliente"]
    single_current = spark.sql(f"""
        SELECT COUNT(*) AS n FROM (
            SELECT id_cliente FROM {dim} GROUP BY id_cliente HAVING SUM(CAST(is_current AS INT)) <> 1
        )
    """).first()["n"]
    overlapping = spark.sql(f"""
        SELECT COUNT(*) AS n
        FROM {dim} a JOIN {dim} b
          ON a.id_cliente = b.id_cliente AND a.cliente_sk < b.cliente_sk
         AND a.valid_from <= b.valid_to AND b.valid_from <= a.valid_to
    """).first()["n"]

    return evaluate_gold(silver_rows, fact.count(), orphans, single_current, overlapping)
