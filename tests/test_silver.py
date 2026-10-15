"""Tests de la capa Silver. Corren sin cluster ni Spark: `pytest tests/`."""

import re
from datetime import date
from pathlib import Path

import pytest

from iol_pipeline.silver import (
    DQ_FLAGS,
    DQ_RULES,
    FERIADOS_AR,
    SEVERITIES,
    DQRule,
    PRICE_DEVIATION_THRESHOLD,
    SILVER_COLUMNS,
    STATUS_FAIL,
    STATUS_OK,
    base_symbol,
    business_calendar,
    classify_instrument,
    evaluate_silver,
    is_business_day,
    liquidation_type,
    next_business_day,
    price_deviation,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
SILVER_DDL = REPO_ROOT / "setup" / "04_silver.sql"
HOLIDAYS = set(FERIADOS_AR)


# --- Calendario (DT-004) -------------------------------------------------------

@pytest.mark.parametrize(
    "day, expected",
    [
        (date(2026, 1, 7), date(2026, 1, 7)),    # miércoles hábil: no se mueve
        (date(2026, 1, 10), date(2026, 1, 12)),  # sábado -> lunes
        (date(2026, 1, 11), date(2026, 1, 12)),  # domingo -> lunes
        (date(2026, 1, 1), date(2026, 1, 2)),    # Año Nuevo (jueves) -> viernes
        (date(2026, 2, 14), date(2026, 2, 18)),  # sábado antes de carnaval -> miércoles
        (date(2026, 2, 16), date(2026, 2, 18)),  # lunes de carnaval -> miércoles
    ],
)
def test_next_business_day(day, expected):
    assert next_business_day(day, HOLIDAYS) == expected


def test_holidays_are_not_business_days():
    assert not any(is_business_day(d, HOLIDAYS) for d in FERIADOS_AR)


def test_business_calendar_covers_range():
    cal = business_calendar(date(2026, 2, 13), date(2026, 2, 18), HOLIDAYS)
    assert [c[0] for c in cal] == [date(2026, 2, d) for d in range(13, 19)]
    assert [c[1] for c in cal] == [True, False, False, False, False, True]
    assert all(c[2] >= c[0] for c in cal)


# --- Clasificación de instrumentos ----------------------------------------------

@pytest.mark.parametrize(
    "simbolo, descripcion, expected",
    [
        ("AAPL", "Cedear Apple Inc.", "CEDEAR"),
        ("AAPLD", "CEDEAR Apple Inc.", "CEDEAR"),
        ("VST", "CEDEARV VISTRA CORPORATION", "CEDEAR"),
        ("AL30", "Bono Rep. Argentina Usd Step Up 2030", "BONO"),
        ("TVPP", "CUPÓN VINCULADO AL PBI $ 2035 (LEY ARG)", "BONO"),
        ("D30A6", "D30A6 VTO 30/04/2026", "BONO"),
        ("S17A6", "Letra Capitalizable del Tesoro", "LETRA"),
        ("X15Y6", "Lt Tesoro Nacional Ajustable Cer", "LETRA"),
        ("YMCJO", "YPF S.A. - CLASE XVIII- VTO. 30/9/2033", "ON"),
        ("GFGC79054F", "GFGC79054F", "OPCION"),
        ("GFGV67054F", "GFGV67054F", "OPCION"),
        ("IRS2W", "Warrant Irsa Vto 13/05/26", "WARRANT"),
        ("YPFD", "YPF", "ACCION"),
        ("GGAL", "Grupo Financiero Galicia S.A", "ACCION"),
        ("BBD", "Banco Bradesco", "ACCION"),  # termina en D pero no es opción ni variante
    ],
)
def test_classify_instrument(simbolo, descripcion, expected):
    assert classify_instrument(simbolo, descripcion) == expected


def test_classify_instrument_handles_missing_description():
    assert classify_instrument("XYZ", None) == "ACCION"


# --- Variantes de liquidación (DT-005) --------------------------------------------

@pytest.mark.parametrize(
    "simbolo, moneda, tipo, base",
    [
        ("AL30D", "USD", "MEP", "AL30"),
        ("AL30C", "USD", "CABLE", "AL30"),
        ("BA.CD", "USD", "MEP", "BA.C"),
        ("AL30", "ARS", "ARS", "AL30"),
        ("YPFD", "ARS", "ARS", "YPFD"),  # ticker real, no variante
        ("GLD", "ARS", "ARS", "GLD"),
        ("XYZ", "USD", "USD_OTRO", "XYZ"),
    ],
)
def test_liquidation_variants(simbolo, moneda, tipo, base):
    assert liquidation_type(simbolo, moneda) == tipo
    assert base_symbol(simbolo, moneda) == base


# --- Reglas de calidad (DT-006) ---------------------------------------------------

def test_price_deviation_is_symmetric():
    assert price_deviation(10, 1) == price_deviation(1, 10) == 10


@pytest.mark.parametrize(
    "precio, mediana, flagged",
    [
        (0.01, 876.32, True),     # placeholder en AL30
        (1.0046, 5355.49, True),  # placeholder en SATL
        (134.55, 477.40, False),  # movimiento real de BHIP (3.5x)
        (900.0, 876.32, False),
    ],
)
def test_price_threshold_separates_placeholders(precio, mediana, flagged):
    assert (price_deviation(precio, mediana) > PRICE_DEVIATION_THRESHOLD) == flagged


# --- Registro de reglas (DT-014) ---------------------------------------------------

def test_rule_names_are_unique():
    names = [rule.name for rule in DQ_RULES]
    assert len(names) == len(set(names))


def test_rule_names_are_upper_snake_case():
    assert all(re.fullmatch(r"[A-Z][A-Z0-9_]*", rule.name) for rule in DQ_RULES)


@pytest.mark.parametrize("rule", DQ_RULES, ids=lambda r: r.name)
def test_rule_is_well_formed(rule):
    assert rule.severity in SEVERITIES
    assert rule.description.strip()
    assert rule.condition.strip()


def test_dq_flags_follow_registry_order():
    assert DQ_FLAGS == tuple(rule.name for rule in DQ_RULES)


def test_rules_are_immutable():
    with pytest.raises(Exception):
        DQ_RULES[0].condition = "TRUE"


def test_rule_parameters_are_reflected_in_condition():
    """Si cambia un umbral, la condición SQL lo refleja (no hay números duplicados a mano)."""
    rule = next(r for r in DQ_RULES if r.name == "PRECIO_FUERA_DE_RANGO")
    assert str(PRICE_DEVIATION_THRESHOLD) in rule.condition
    assert "'OPCION'" in rule.condition


# --- Ejecución de las reglas con Spark local (opcional) -------------------------------
# Se saltea si PySpark no está instalado; requiere Java.

@pytest.fixture(scope="module")
def spark():
    pyspark = pytest.importorskip("pyspark")
    session = (
        pyspark.sql.SparkSession.builder.master("local[1]")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_apply_dq_rules_flags_and_status(spark):
    from iol_pipeline.silver import apply_dq_rules

    rows = [
        # es_dia_habil, tipo_instrumento, _n_obs, _desvio_precio, monto, _monto_limite
        ("ok", True, "BONO", 100, 1.1, 10.0, 100.0),
        ("inhabil", False, "BONO", 100, 1.1, 10.0, 100.0),
        ("placeholder", True, "BONO", 100, 87631.9, 10.0, 100.0),
        ("opcion", True, "OPCION", 100, 50.0, 10.0, 100.0),       # opciones excluidas
        ("pocas_obs", True, "BONO", 3, 50.0, 10.0, 100.0),         # mediana no confiable
        ("sin_ref", True, "BONO", 100, None, 10.0, 100.0),         # NULL no marca
        ("doble", False, "CEDEAR", 100, 1.0, 500.0, 100.0),
    ]
    df = spark.createDataFrame(
        rows,
        "caso STRING, es_dia_habil BOOLEAN, tipo_instrumento STRING, _n_obs LONG, "
        "_desvio_precio DOUBLE, monto DOUBLE, _monto_limite DOUBLE",
    )
    result = {r["caso"]: (r["dq_flags"], r["dq_status"]) for r in apply_dq_rules(df, DQ_RULES).collect()}

    assert result["ok"] == ([], "OK")
    assert result["inhabil"] == (["DIA_INHABIL"], "WARN")
    assert result["placeholder"] == (["PRECIO_FUERA_DE_RANGO"], "WARN")
    assert result["opcion"] == ([], "OK")
    assert result["pocas_obs"] == ([], "OK")
    assert result["sin_ref"] == ([], "OK")
    assert result["doble"] == (["DIA_INHABIL", "MONTO_EXTREMO"], "WARN")


def test_apply_dq_rules_error_severity(spark):
    from iol_pipeline.silver import apply_dq_rules

    rules = [
        DQRule("A", "warn", "x > 1", "WARN"),
        DQRule("B", "error", "x > 2", "ERROR"),
    ]
    df = spark.createDataFrame([(0,), (2,), (3,)], "x INT")
    status = [r["dq_status"] for r in apply_dq_rules(df, rules).orderBy("x").collect()]
    assert status == ["OK", "WARN", "ERROR"]


# --- Evaluación de la corrida ------------------------------------------------------

def test_evaluate_silver_ok_with_flags():
    result = evaluate_silver("t", 100, 98, 2, 0, 15)
    assert result.status == STATUS_OK


@pytest.mark.parametrize(
    "bronze, silver, dups, sin_cal",
    [
        (100, 97, 2, 0),  # se perdió una fila
        (100, 100, 2, 0),  # aparecieron filas
        (100, 100, 0, 3),  # fechas fuera del calendario
    ],
)
def test_evaluate_silver_fail(bronze, silver, dups, sin_cal):
    assert evaluate_silver("t", bronze, silver, dups, sin_cal, 0).status == STATUS_FAIL


# --- Schema ---------------------------------------------------------------------

def _ddl_columns(table: str) -> list[str]:
    body = SILVER_DDL.read_text().split(f"CREATE TABLE IF NOT EXISTS {table}", 1)[1]
    body = body.split("(", 1)[1].split("\n)", 1)[0]
    return re.findall(r"^\s+([a-zA-Z_]\w*)\s+[A-Z]", body, re.MULTILINE)


def test_schema_matches_silver_ddl():
    assert SILVER_COLUMNS == _ddl_columns("workspace.silver.transacciones")
