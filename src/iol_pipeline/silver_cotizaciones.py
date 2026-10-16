"""Cotizaciones en Silver: unifica las fuentes y asigna el precio de mercado (Task 04).

    bronze.data912_cotizaciones ─ conform_data912 (÷100 en renta fija) ─┐
                                                                         ├─▶ silver.cotizaciones (simbolo, fecha, fuente)
    bronze.yahoo_cotizaciones   ─ conform_yahoo (revierte splits) ──────┘      + es_preferida según la prioridad del registro

    silver.cotizaciones (preferidas) + fecha_operativa ─▶ precio de mercado de cada operación
        EXACTO          cierre del mismo día
        IMPUTADO        último cierre hasta IMPUTATION_MAX_BUSINESS_DAYS días hábiles antes (DT-020)
        SIN_COTIZACION  sin cierre en esa ventana

La lógica pura (calendario hábil, clasificación del precio) no depende de Spark.
"""

from datetime import date, timedelta

from iol_pipeline.silver import FERIADOS_AR, is_business_day

# --- Normalización por fuente (DT-021) ----------------------------------------------

# data912 cotiza la renta fija cada 100 VN; el dataset, por 1 VN.
RENTA_FIJA_TYPES = ("BONO", "LETRA", "ON")
DATA912_FIXED_INCOME_SCALE = 0.01

# --- Imputación (DT-020) --------------------------------------------------------------

IMPUTATION_MAX_BUSINESS_DAYS = 5

PRICE_EXACT = "EXACTO"
PRICE_IMPUTED = "IMPUTADO"
PRICE_MISSING = "SIN_COTIZACION"


def business_day_index(start: date, end: date, holidays: set[date] | None = None) -> list[tuple[date, int]]:
    """Índice de día hábil para cada fecha de [start, end]: la cantidad de días
    hábiles transcurridos desde `start`. Un día inhábil comparte el índice del
    hábil anterior. La diferencia de índices entre dos fechas es la distancia
    en días hábiles."""
    holidays = set(FERIADOS_AR) if holidays is None else holidays
    rows, index = [], 0
    for i in range((end - start).days + 1):
        d = start + timedelta(days=i)
        if is_business_day(d, holidays):
            index += 1
        rows.append((d, index))
    return rows


def classify_price(business_days_gap: int | None, max_gap: int = IMPUTATION_MAX_BUSINESS_DAYS) -> str:
    if business_days_gap is None or business_days_gap > max_gap:
        return PRICE_MISSING
    return PRICE_EXACT if business_days_gap == 0 else PRICE_IMPUTED


def price_deviation_pct(precio: float, precio_mercado: float | None) -> float | None:
    """Desvío del precio operado contra el de mercado, en %."""
    if not precio_mercado:
        return None
    return 100 * (precio - precio_mercado) / precio_mercado


# --- Schemas de salida ------------------------------------------------------------------
# Deben coincidir con setup/06_cotizaciones.sql (lo verifica tests/).

SILVER_QUOTES_COLUMNS = [
    "simbolo", "fecha", "fuente", "prioridad", "es_preferida",
    "apertura", "maximo", "minimo", "cierre", "volumen",
    "cierre_fuente", "factor_ajuste", "_extracted_at", "_processed_at",
]

MARKET_PRICE_TYPES = {
    "precio_mercado": "decimal(20,6)",
    "fecha_precio_mercado": "date",
    "fuente_precio": "string",
    "dias_habiles_desfase": "int",
    "tipo_precio": "string",
    "desvio_vs_mercado_pct": "double",
}
MARKET_PRICE_COLUMNS = list(MARKET_PRICE_TYPES)

# Deben coincidir con gold.cobertura_cotizaciones en setup/05_gold.sql.
COVERAGE_COLUMNS = [
    "simbolo", "tipo_instrumento", "tipo_liquidacion", "operaciones", "ops_precio_exacto",
    "ops_precio_imputado", "ops_sin_cotizacion", "pct_con_precio", "fuente_principal",
    "motivo_sin_cotizacion", "_processed_at",
]


# --- Spark ------------------------------------------------------------------------------------
# PySpark se importa dentro de las funciones para que los tests corran sin Spark.


def latest_extraction(bronze_df):
    """Última versión de cada (símbolo, fecha) según la extracción más reciente.

    Si un símbolo falló en la última extracción pero había salido bien antes,
    se conserva el dato anterior: una falla puntual de la API no borra precios.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    w = Window.partitionBy("simbolo", "date").orderBy(F.desc("_extracted_at"), F.desc("_source_file"))
    return bronze_df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def _conformed(df, apertura, maximo, minimo, cierre_fuente, volumen, factor, fuente):
    from pyspark.sql import functions as F

    return df.select(
        F.col("simbolo"),
        F.col("date").alias("fecha"),
        F.lit(fuente).alias("fuente"),
        (apertura * factor).alias("apertura"),
        (maximo * factor).alias("maximo"),
        (minimo * factor).alias("minimo"),
        (cierre_fuente * factor).alias("cierre"),
        volumen.alias("volumen"),
        cierre_fuente.alias("cierre_fuente"),
        factor.alias("factor_ajuste"),
        F.col("_extracted_at"),
    )


def conform_data912(bronze_df, instrument_types_df):
    """data912 publica precios sin ajustar; la renta fija viene cada 100 VN."""
    from pyspark.sql import functions as F

    df = latest_extraction(bronze_df).join(instrument_types_df, "simbolo", "left")
    factor = F.when(F.col("tipo_instrumento").isin(*RENTA_FIJA_TYPES), F.lit(DATA912_FIXED_INCOME_SCALE)).otherwise(F.lit(1.0))
    return _conformed(df, F.col("o"), F.col("h"), F.col("l"), F.col("c"), F.col("v"), factor, "data912")


def conform_yahoo(bronze_df, instrument_types_df=None):
    """Yahoo ajusta retroactivamente por splits: el precio real de un día es el
    publicado multiplicado por los splits posteriores a ese día.

    Ej. SPY 3:1 el 29/05/2026: el cierre del 05/01/2026 publicado (17.650) x 3
    = 52.950, que es lo que efectivamente cotizaba. La fila del día del split
    ya está en la escala nueva, por eso solo cuentan los splits estrictamente
    posteriores.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    df = latest_extraction(bronze_df)
    after = Window.partitionBy("simbolo").orderBy(F.desc("date")).rowsBetween(Window.unboundedPreceding, -1)
    split = F.when(F.col("stock_splits") > 0, F.col("stock_splits")).otherwise(F.lit(1.0))
    factor = F.round(F.exp(F.coalesce(F.sum(F.log(split)).over(after), F.lit(0.0))), 6)
    df = df.withColumn("_factor", factor)
    return _conformed(df, F.col("open"), F.col("high"), F.col("low"), F.col("close"), F.col("volume"),
                      F.col("_factor"), "yahoo")


# Conformador de cada fuente del registro (iol_pipeline.sources.QUOTE_SOURCES).
CONFORMERS = {
    "data912": conform_data912,
    "yahoo": conform_yahoo,
}


def priorities_df(spark, sources):
    rows = [(s.name, tipo, p) for s in sources for tipo, p in s.priority.items()]
    return spark.createDataFrame(rows, "fuente STRING, tipo_instrumento STRING, prioridad INT")


def build_silver_quotes(spark, bronze_tables: dict, instrument_types_df, sources, processed_at):
    """Une las fuentes conformadas y marca la preferida por (símbolo, fecha):
    la de menor prioridad para el tipo de instrumento del símbolo."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    conformed = None
    for source in sources:
        df = CONFORMERS[source.name](spark.table(bronze_tables[source.name]), instrument_types_df)
        conformed = df if conformed is None else conformed.unionByName(df)

    ranked = (
        conformed.join(instrument_types_df, "simbolo", "inner")
        .join(F.broadcast(priorities_df(spark, sources)), ["fuente", "tipo_instrumento"], "inner")
        .where(F.col("cierre").isNotNull() & (F.col("cierre") > 0))
    )
    w = Window.partitionBy("simbolo", "fecha").orderBy("prioridad", "fuente")
    return (
        ranked.withColumn("es_preferida", F.row_number().over(w) == 1)
        .withColumn("_processed_at", F.lit(processed_at).cast("timestamp"))
        .select(*SILVER_QUOTES_COLUMNS)
    )


def business_day_index_df(spark, start: date, end: date):
    return spark.createDataFrame(business_day_index(start, end), "fecha DATE, indice_habil INT")


def match_market_prices(spark, operations_df, quotes_df, max_gap: int = IMPUTATION_MAX_BUSINESS_DAYS):
    """Precio de mercado de cada operación (as-of join sobre días hábiles).

    `operations_df`: id_transaccion, simbolo, fecha_operativa, precio.
    `quotes_df`: silver.cotizaciones (se usan solo las preferidas).
    Devuelve una fila por operación, con `tipo_precio` = EXACTO, IMPUTADO o
    SIN_COTIZACION.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    preferred = quotes_df.where("es_preferida").select(
        "simbolo", F.col("fecha").alias("fecha_precio_mercado"), F.col("cierre").alias("precio_mercado"),
        F.col("fuente").alias("fuente_precio"),
    )
    bounds = operations_df.agg(F.min("fecha_operativa").alias("lo"), F.max("fecha_operativa").alias("hi")).first()
    # Margen en días corridos que cubre holgadamente `max_gap` días hábiles.
    lookback = timedelta(days=max_gap * 2 + 10)
    index = business_day_index_df(spark, bounds["lo"] - lookback, bounds["hi"])
    op_index = index.select(F.col("fecha").alias("fecha_operativa"), F.col("indice_habil").alias("_idx_op"))
    quote_index = index.select(F.col("fecha").alias("fecha_precio_mercado"), F.col("indice_habil").alias("_idx_q"))

    ops = operations_df.select("id_transaccion", "simbolo", "fecha_operativa", "precio").join(
        F.broadcast(op_index), "fecha_operativa", "left"
    )
    candidates = (
        ops.alias("o")
        .join(
            preferred.alias("q"),
            (F.col("o.simbolo") == F.col("q.simbolo"))
            & (F.col("q.fecha_precio_mercado") <= F.col("o.fecha_operativa"))
            & (F.col("q.fecha_precio_mercado") >= F.date_sub(F.col("o.fecha_operativa"), lookback.days)),
            "left",
        )
        .join(F.broadcast(quote_index), "fecha_precio_mercado", "left")
        .withColumn("dias_habiles_desfase", F.col("_idx_op") - F.col("_idx_q"))
        # Un candidato fuera de la ventana hábil no sirve: se descarta su precio.
        .withColumn("_valido", F.col("dias_habiles_desfase") <= max_gap)
    )
    w = Window.partitionBy("id_transaccion").orderBy(F.col("_valido").desc_nulls_last(),
                                                       F.col("fecha_precio_mercado").desc_nulls_last())
    best = candidates.withColumn("_rn", F.row_number().over(w)).where("_rn = 1")

    valid = F.coalesce(F.col("_valido"), F.lit(False))
    return best.select(
        F.col("id_transaccion"),
        F.when(valid, F.col("precio_mercado")).cast("decimal(20,6)").alias("precio_mercado"),
        F.when(valid, F.col("fecha_precio_mercado")).alias("fecha_precio_mercado"),
        F.when(valid, F.col("fuente_precio")).alias("fuente_precio"),
        F.when(valid, F.col("dias_habiles_desfase")).cast("int").alias("dias_habiles_desfase"),
        F.when(~valid, F.lit(PRICE_MISSING))
        .when(F.col("dias_habiles_desfase") == 0, F.lit(PRICE_EXACT))
        .otherwise(F.lit(PRICE_IMPUTED)).alias("tipo_precio"),
        F.when(valid, F.expr(
            "try_divide(CAST(precio AS DOUBLE) - precio_mercado, precio_mercado) * 100"
        )).alias("desvio_vs_mercado_pct"),
    )


def coverage_by_symbol(fact_df, dim_instrumento_df, errors_df, sources, processed_at):
    """Cobertura de precio de mercado por símbolo, con el motivo cuando falta.

    El motivo sale de la última extracción de cada fuente en `bronze.errores_api`
    (ej. "data912: NOT_FOUND; yahoo: NOT_FOUND"), o indica que ninguna fuente
    cubre el tipo de instrumento, o que hubo datos pero no en la ventana hábil.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    covered_types = sorted({tipo for s in sources for tipo in s.priority})
    ops = fact_df.join(dim_instrumento_df.select("instrumento_key", "simbolo", "tipo_instrumento", "tipo_liquidacion"),
                       "instrumento_key")
    by_symbol = ops.groupBy("simbolo", "tipo_instrumento", "tipo_liquidacion").agg(
        F.count("*").alias("operaciones"),
        F.sum((F.col("tipo_precio") == PRICE_EXACT).cast("int")).alias("ops_precio_exacto"),
        F.sum((F.col("tipo_precio") == PRICE_IMPUTED).cast("int")).alias("ops_precio_imputado"),
        F.sum((F.col("tipo_precio") == PRICE_MISSING).cast("int")).alias("ops_sin_cotizacion"),
        F.mode("fuente_precio").alias("fuente_principal"),
    )

    last = Window.partitionBy("fuente", "simbolo").orderBy(F.desc("_extracted_at"))
    latest_errors = (
        errors_df.withColumn("_rn", F.row_number().over(last)).where("_rn = 1")
        .groupBy("simbolo")
        .agg(F.concat_ws("; ", F.sort_array(F.collect_list(F.concat_ws(": ", "fuente", "error_type")))).alias("_errores"))
    )
    motivo = (
        F.when(F.col("ops_sin_cotizacion") == 0, F.lit(None))
        .when(~F.col("tipo_instrumento").isin(*covered_types), F.concat(F.lit("Sin fuente para "), F.col("tipo_instrumento")))
        .when(F.col("_errores").isNotNull() & (F.col("ops_precio_exacto") + F.col("ops_precio_imputado") == 0), F.col("_errores"))
        .when(F.col("ops_precio_exacto") + F.col("ops_precio_imputado") == 0, F.lit("La fuente no tiene cierres en el período"))
        .otherwise(F.lit(f"Sin cierre en los {IMPUTATION_MAX_BUSINESS_DAYS} días hábiles previos a algunas operaciones"))
    )
    return (
        by_symbol.join(latest_errors, "simbolo", "left")
        .withColumn("pct_con_precio", F.round(100 * (F.col("ops_precio_exacto") + F.col("ops_precio_imputado")) / F.col("operaciones"), 2))
        .withColumn("motivo_sin_cotizacion", motivo)
        .withColumn("_processed_at", F.lit(processed_at).cast("timestamp"))
        .select(*COVERAGE_COLUMNS)
    )
