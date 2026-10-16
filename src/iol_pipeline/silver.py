"""Transformación Bronze → Silver de transacciones.

Silver limpia, tipa y enriquece cada transacción, y marca los problemas de
calidad en `dq_flags` sin descartar registros (DT-003). Las únicas filas que
no pasan a Silver son los duplicados de `id_transaccion`, que siguen
trazables en Bronze y se reportan como métrica.

Las reglas de negocio (calendario, clasificación de instrumentos, variantes
de liquidación) se declaran como datos y tienen una versión en Python puro,
para testearlas sin Spark. Las expresiones Spark se construyen a partir de
las mismas reglas, así no pueden divergir.
"""

import re
from dataclasses import asdict, dataclass
from datetime import date, timedelta

# --- Mercado -------------------------------------------------------------------

MARKET_TZ = "America/Argentina/Buenos_Aires"

# Horario de negociación continua de BYMA (hora local): [11:00, 17:00).
MARKET_OPEN_HOUR = 11
MARKET_CLOSE_HOUR = 17

# Feriados nacionales con mercado cerrado dentro del período del dataset
# (ene-mar 2026). Fuente: calendario oficial de feriados de Argentina.
# Para procesar otros períodos hay que extender esta lista (DT-004).
FERIADOS_AR: dict[date, str] = {
    date(2026, 1, 1): "Año Nuevo",
    date(2026, 2, 16): "Carnaval",
    date(2026, 2, 17): "Carnaval",
    date(2026, 3, 24): "Día Nacional de la Memoria por la Verdad y la Justicia",
}


def is_business_day(day: date, holidays: set[date]) -> bool:
    return day.weekday() < 5 and day not in holidays


def next_business_day(day: date, holidays: set[date]) -> date:
    """El mismo día si es hábil; si no, el siguiente día hábil."""
    while not is_business_day(day, holidays):
        day += timedelta(days=1)
    return day


def business_calendar(start: date, end: date, holidays: set[date]) -> list[tuple[date, bool, date]]:
    """Calendario [start, end] con (fecha, es_dia_habil, fecha_operativa)."""
    days = (end - start).days + 1
    return [
        (d, is_business_day(d, holidays), next_business_day(d, holidays))
        for d in (start + timedelta(days=i) for i in range(days))
    ]


# --- Clasificación de instrumentos -------------------------------------------------

# Reglas evaluadas en orden; la primera que matchea define el tipo. Las regex
# se evalúan con semántica de "search" (igual que `rlike` en Spark) sobre el
# símbolo o sobre la descripción normalizada (trim + mayúsculas).
# Lo que no matchea ninguna regla se clasifica como ACCION (DT-011).
INSTRUMENT_RULES: list[tuple[str, str, str]] = [
    # Opciones: subyacente + C/V + strike + mes (GFGC79054F, ALUC1000JU, BHIC420.AB).
    # Exigir el sufijo de mes evita falsos positivos como ABEV3 (CEDEAR) o BPOC7 (BOPREAL).
    ("OPCION", "simbolo", r"^[A-Z]{3}[CV][0-9]+(\.[0-9]+)?\.?[A-Z]{1,2}$"),
    ("CEDEAR", "descripcion", r"^CEDEAR"),  # incluye el typo "CEDEARV"
    ("BONO", "descripcion", r"^(BONOS?|BONCAP|BOPREAL)\b"),
    ("ON", "descripcion", r"^(ON|O\.N\.)\b"),
    ("LETRA", "descripcion", r"^(LETRAS?\b|LT\b|L\.)"),
    ("WARRANT", "descripcion", r"^WARRANT\b"),
    ("ON", "descripcion", r"\b(CLASE|REGS)\b"),  # ONs corporativas sin prefijo
    ("BONO", "descripcion", r"\bVTO\b|[0-9]%|TITULOS DE DEUDA|^T\.D\.|CUP[OÓ]N"),
]
DEFAULT_INSTRUMENT = "ACCION"


def normalize_description(descripcion: str | None) -> str:
    return (descripcion or "").strip().upper()


def classify_instrument(simbolo: str, descripcion: str | None) -> str:
    fields = {"simbolo": simbolo or "", "descripcion": normalize_description(descripcion)}
    for tipo, field, pattern in INSTRUMENT_RULES:
        if re.search(pattern, fields[field]):
            return tipo
    return DEFAULT_INSTRUMENT


# --- Variantes de liquidación (DT-005) ------------------------------------------

# La moneda es el discriminador: toda operación en USD usa un símbolo con
# sufijo D (MEP) o C (Cable). Un ticker en ARS que termina en D o C (YPFD,
# GLD, AMD) es un ticker real, no una variante.
LIQUIDATION_SUFFIXES = {"D": "MEP", "C": "CABLE"}


def liquidation_type(simbolo: str, moneda: str) -> str:
    if moneda != "USD":
        return "ARS"
    return LIQUIDATION_SUFFIXES.get(simbolo[-1:], "USD_OTRO")


def base_symbol(simbolo: str, moneda: str) -> str:
    if moneda == "USD" and simbolo[-1:] in LIQUIDATION_SUFFIXES:
        return simbolo[:-1]
    return simbolo


# --- Reglas de calidad (DT-006) --------------------------------------------------

# Un precio está fuera de rango si se desvía más de N veces de la mediana de
# su símbolo y moneda. N=10 se calibró con los datos: los placeholders
# (0.01, ~1.00) se desvían más de 100x y los movimientos reales del trimestre
# no superan 3.6x. Se exige un mínimo de operaciones para que la mediana sea
# representativa, y no aplica a opciones por su volatilidad.
PRICE_DEVIATION_THRESHOLD = 10.0
PRICE_MIN_OBSERVATIONS = 5
PRICE_RULE_EXCLUDED_TYPES = ("OPCION",)

# Monto extremo: por encima de este percentil dentro de su tipo de instrumento.
AMOUNT_EXTREME_PERCENTILE = 0.999

SEVERITY_WARN = "WARN"    # la fila se marca y sigue disponible
SEVERITY_ERROR = "ERROR"  # la fila se marca y no debería usarse en Gold
SEVERITIES = (SEVERITY_WARN, SEVERITY_ERROR)


@dataclass(frozen=True)
class DQRule:
    """Regla de calidad a nivel registro (DT-014).

    `condition` es una expresión SQL evaluada sobre cada fila de Silver
    (incluidas las columnas auxiliares `_n_obs`, `_desvio_precio` y
    `_monto_limite`): TRUE significa que la fila tiene el problema.
    """

    name: str
    description: str
    condition: str
    severity: str


# Registro de reglas: para agregar una regla, sumarla a esta lista. La
# transformación arma `dq_flags` y `dq_status` a partir de ella.
DQ_RULES: list[DQRule] = [
    DQRule(
        name="DIA_INHABIL",
        description="Operación en fin de semana o feriado (hora local); se imputa al siguiente día hábil",
        condition="NOT es_dia_habil",
        severity=SEVERITY_WARN,
    ),
    DQRule(
        name="PRECIO_FUERA_DE_RANGO",
        description=(
            f"Precio a más de {PRICE_DEVIATION_THRESHOLD:g}x de la mediana de su símbolo y moneda "
            f"(mínimo {PRICE_MIN_OBSERVATIONS} operaciones, excluye opciones)"
        ),
        condition=(
            f"tipo_instrumento NOT IN ({', '.join(repr(t) for t in PRICE_RULE_EXCLUDED_TYPES)}) "
            f"AND _n_obs >= {PRICE_MIN_OBSERVATIONS} "
            f"AND _desvio_precio > {PRICE_DEVIATION_THRESHOLD}"
        ),
        severity=SEVERITY_WARN,
    ),
    DQRule(
        name="MONTO_EXTREMO",
        description=f"Monto por encima del p{AMOUNT_EXTREME_PERCENTILE * 100:g} de su tipo de instrumento",
        condition="monto > _monto_limite",
        severity=SEVERITY_WARN,
    ),
]

DQ_FLAGS = tuple(rule.name for rule in DQ_RULES)


def price_deviation(precio: float, mediana: float) -> float:
    """Desvío simétrico: 10x arriba y 10x abajo de la mediana valen lo mismo."""
    ratio = precio / mediana
    return max(ratio, 1 / ratio)


# --- Schema de salida ------------------------------------------------------------

# Debe coincidir con setup/04_silver.sql (lo verifica tests/test_silver.py).
SILVER_COLUMNS: list[str] = [
    "id_transaccion",
    "fecha_hora_utc",
    "fecha_hora_local",
    "fecha_local",
    "fecha_operativa",
    "es_dia_habil",
    "en_horario_mercado",
    "id_cliente",
    "tipo_operacion",
    "canal",
    "simbolo",
    "simbolo_base",
    "tipo_liquidacion",
    "tipo_instrumento",
    "descripcion_titulo",
    "moneda",
    "cantidad",
    "precio",
    "monto",
    "precio_referencia",
    "dq_flags",
    "dq_status",
    "_source_file",
    "_ingested_at",
    "_processed_at",
]


# --- Evaluación de la corrida ------------------------------------------------------

STATUS_OK = "OK"
STATUS_FAIL = "FAIL"


@dataclass
class SilverResult:
    table: str
    bronze_rows: int
    silver_rows: int
    duplicates_removed: int
    rows_without_calendar: int
    rows_with_flags: int
    status: str
    message: str

    def to_dict(self) -> dict:
        return asdict(self)


def evaluate_silver(
    table: str,
    bronze_rows: int,
    silver_rows: int,
    duplicates_removed: int,
    rows_without_calendar: int,
    rows_with_flags: int,
) -> SilverResult:
    """Controles de integridad de Silver.

    - FAIL si no cierra el balance Bronze = Silver + duplicados (se perdieron
      o inventaron filas), o si hay fechas fuera del calendario.
    - Los flags de calidad no hacen fallar la corrida: son el comportamiento
      esperado (marcar, no descartar) y se reportan en el mensaje.
    """
    if bronze_rows != silver_rows + duplicates_removed:
        status, message = STATUS_FAIL, (
            f"Balance no cierra: bronze={bronze_rows}, silver={silver_rows}, "
            f"duplicados={duplicates_removed}"
        )
    elif rows_without_calendar > 0:
        status, message = STATUS_FAIL, (
            f"{rows_without_calendar} filas sin fecha_operativa: extender el calendario"
        )
    else:
        status, message = STATUS_OK, (
            f"{silver_rows} filas en silver, {duplicates_removed} duplicados removidos, "
            f"{rows_with_flags} filas con flags de calidad"
        )
    return SilverResult(
        table=table,
        bronze_rows=bronze_rows,
        silver_rows=silver_rows,
        duplicates_removed=duplicates_removed,
        rows_without_calendar=rows_without_calendar,
        rows_with_flags=rows_with_flags,
        status=status,
        message=message,
    )


# --- Spark -------------------------------------------------------------------------
# PySpark se importa dentro de las funciones para que los tests corran sin Spark.


def _instrument_type_expr():
    from pyspark.sql import functions as F

    fields = {
        "simbolo": F.col("simbolo"),
        "descripcion": F.upper(F.trim(F.col("descripcion_titulo"))),
    }
    expr = None
    for tipo, field, pattern in INSTRUMENT_RULES:
        cond = fields[field].rlike(pattern)
        expr = F.when(cond, F.lit(tipo)) if expr is None else expr.when(cond, F.lit(tipo))
    return expr.otherwise(F.lit(DEFAULT_INSTRUMENT))


def _liquidation_type_expr():
    from pyspark.sql import functions as F

    suffix = F.substring(F.col("simbolo"), -1, 1)
    expr = F.when(F.col("moneda") != "USD", F.lit("ARS"))
    for suf, tipo in LIQUIDATION_SUFFIXES.items():
        expr = expr.when(suffix == suf, F.lit(tipo))
    return expr.otherwise(F.lit("USD_OTRO"))


def _base_symbol_expr():
    from pyspark.sql import functions as F

    is_variant = (F.col("moneda") == "USD") & F.substring(F.col("simbolo"), -1, 1).isin(
        *LIQUIDATION_SUFFIXES
    )
    return F.when(is_variant, F.expr("substring(simbolo, 1, length(simbolo) - 1)")).otherwise(
        F.col("simbolo")
    )


def calendar_df(spark, start: date, end: date, holidays: dict[date, str] = FERIADOS_AR):
    rows = business_calendar(start, end, set(holidays))
    return spark.createDataFrame(rows, "fecha DATE, es_dia_habil BOOLEAN, fecha_operativa DATE")


def holidays_df(spark, holidays: dict[date, str] = FERIADOS_AR):
    return spark.createDataFrame(sorted(holidays.items()), "fecha DATE, descripcion STRING")


def deduplicate(bronze_df):
    """Una fila por id_transaccion: la versión ingerida más recientemente."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    w = Window.partitionBy("id_transaccion").orderBy(
        F.col("_ingested_at").desc(), F.col("_source_file").desc()
    )
    return bronze_df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def transform_transactions(spark, bronze_df, processed_at):
    """Bronze (ya deduplicado) → Silver: renombra, tipa, enriquece y marca calidad."""
    from pyspark.sql import functions as F

    df = bronze_df.select(
        F.col("id_transaccion"),
        F.col("fecha").alias("fecha_hora_utc"),
        F.from_utc_timestamp(F.col("fecha"), MARKET_TZ).alias("fecha_hora_local"),
        F.col("id_cliente"),
        F.upper(F.trim(F.col("tipoTran"))).alias("tipo_operacion"),
        F.trim(F.col("origen")).alias("canal"),
        F.trim(F.col("simbolo_titulo")).alias("simbolo"),
        F.trim(F.col("descripcion_titulo")).alias("descripcion_titulo"),
        F.upper(F.trim(F.col("moneda"))).alias("moneda"),
        F.col("cantidad"),
        F.col("precio"),
        F.col("_source_file"),
        F.col("_ingested_at"),
    )

    df = (
        df.withColumn("fecha_local", F.to_date("fecha_hora_local"))
        .withColumn("simbolo_base", _base_symbol_expr())
        .withColumn("tipo_liquidacion", _liquidation_type_expr())
        .withColumn("tipo_instrumento", _instrument_type_expr())
        .withColumn("monto", (F.col("cantidad") * F.col("precio")).cast("decimal(24,6)"))
    )

    # Calendario: fecha_operativa = siguiente día hábil (DT-004).
    bounds = df.agg(F.min("fecha_local").alias("lo"), F.max("fecha_local").alias("hi")).first()
    cal = calendar_df(spark, bounds["lo"], bounds["hi"] + timedelta(days=15))
    df = df.join(F.broadcast(cal.withColumnRenamed("fecha", "fecha_local")), "fecha_local", "left")

    hour = F.hour("fecha_hora_local")
    df = df.withColumn(
        "en_horario_mercado",
        F.col("es_dia_habil") & (hour >= MARKET_OPEN_HOUR) & (hour < MARKET_CLOSE_HOUR),
    )

    # Referencias estadísticas sobre todo el histórico (DT-006).
    price_ref = df.groupBy("simbolo", "moneda").agg(
        F.expr("percentile(precio, 0.5)").cast("decimal(20,6)").alias("precio_referencia"),
        F.count("*").alias("_n_obs"),
    )
    amount_ref = df.groupBy("tipo_instrumento").agg(
        F.expr(f"percentile(monto, {AMOUNT_EXTREME_PERCENTILE})").alias("_monto_limite")
    )
    df = df.join(price_ref, ["simbolo", "moneda"], "left").join(
        F.broadcast(amount_ref), "tipo_instrumento", "left"
    )

    # try_divide sobre doubles: con ANSI activo (serverless) una división por
    # cero o un overflow decimal harían fallar la query en lugar de dar NULL.
    ratio = "try_divide(CAST(precio AS DOUBLE), CAST(precio_referencia AS DOUBLE))"
    df = df.withColumn("_desvio_precio", F.expr(f"greatest({ratio}, try_divide(1.0D, {ratio}))"))

    df = apply_dq_rules(df, DQ_RULES).withColumn(
        "_processed_at", F.lit(processed_at).cast("timestamp")
    )
    return df.select(*SILVER_COLUMNS)


def apply_dq_rules(df, rules: list[DQRule]):
    """Agrega `dq_flags` (reglas que dieron TRUE) y `dq_status` (la peor severidad).

    Una condición que evalúa a NULL (por ejemplo, sin precio de referencia)
    no marca la fila: solo se marca lo que se puede afirmar.
    """
    from pyspark.sql import functions as F

    flags = F.array(*[F.when(F.expr(rule.condition), F.lit(rule.name)) for rule in rules])
    error_flags = [rule.name for rule in rules if rule.severity == SEVERITY_ERROR]

    status = F.when(F.size("dq_flags") == 0, F.lit("OK"))
    if error_flags:
        status = status.when(
            F.arrays_overlap("dq_flags", F.array(*map(F.lit, error_flags))), F.lit(SEVERITY_ERROR)
        )
    return df.withColumn("dq_flags", F.filter(flags, lambda x: x.isNotNull())).withColumn(
        "dq_status", status.otherwise(F.lit(SEVERITY_WARN))
    )


def build_silver(spark, bronze_table: str, silver_table: str, processed_at) -> SilverResult:
    """Reconstruye Silver completo desde Bronze (DT-012) y valida el resultado."""
    from pyspark.sql import functions as F

    bronze = spark.table(bronze_table)
    deduped = deduplicate(bronze)
    silver = transform_transactions(spark, deduped, processed_at)
    silver.writeTo(silver_table).overwrite(F.lit(True))

    written = spark.table(silver_table)
    stats = written.agg(
        F.count("*").alias("silver_rows"),
        F.sum(F.col("fecha_operativa").isNull().cast("int")).alias("sin_calendario"),
        F.sum((F.size("dq_flags") > 0).cast("int")).alias("con_flags"),
    ).first()
    bronze_rows = bronze.count()
    unique_ids = bronze.select("id_transaccion").distinct().count()

    return evaluate_silver(
        table=silver_table,
        bronze_rows=bronze_rows,
        silver_rows=stats["silver_rows"],
        duplicates_removed=bronze_rows - unique_ids,
        rows_without_calendar=stats["sin_calendario"] or 0,
        rows_with_flags=stats["con_flags"] or 0,
    )
