"""Tests de cotizaciones en Silver: normalización, imputación y cobertura.

Los de lógica pura corren sin Spark; los que ejecutan las transformaciones se
saltean si PySpark no está instalado.
"""

import re
from datetime import date, datetime
from pathlib import Path

import pytest

from iol_pipeline.silver_cotizaciones import (
    CONFORMERS,
    COVERAGE_COLUMNS,
    IMPUTATION_MAX_BUSINESS_DAYS,
    PRICE_EXACT,
    PRICE_IMPUTED,
    PRICE_MISSING,
    SILVER_QUOTES_COLUMNS,
    business_day_index,
    classify_price,
    price_deviation_pct,
)
from iol_pipeline.sources import QUOTE_SOURCES

REPO_ROOT = Path(__file__).resolve().parents[1]


def _ddl_columns(path: Path, table: str) -> list[str]:
    body = path.read_text().split(f"CREATE TABLE IF NOT EXISTS {table}", 1)[1]
    body = body.split("(", 1)[1].split("\n)", 1)[0]
    return re.findall(r"^\s+([a-zA-Z_]\w*)\s+[A-Z]", body, re.MULTILINE)


# --- Registro y schemas ------------------------------------------------------------------

def test_every_registered_source_has_a_conformer():
    assert {s.name for s in QUOTE_SOURCES} == set(CONFORMERS)


def test_silver_quotes_schema_matches_ddl():
    ddl = _ddl_columns(REPO_ROOT / "setup" / "06_cotizaciones.sql", "workspace.silver.cotizaciones")
    assert SILVER_QUOTES_COLUMNS == ddl


def test_coverage_schema_matches_ddl():
    ddl = _ddl_columns(REPO_ROOT / "setup" / "05_gold.sql", "workspace.gold.cobertura_cotizaciones")
    assert COVERAGE_COLUMNS == ddl


# --- Calendario hábil e imputación (DT-020) ---------------------------------------------------

def test_business_day_index_skips_weekends_and_holidays():
    index = dict(business_day_index(date(2026, 2, 12), date(2026, 2, 18)))
    # jue 12, vie 13 | sáb 14, dom 15, lun 16 y mar 17 (carnaval) | mié 18
    assert [index[date(2026, 2, d)] for d in range(12, 19)] == [1, 2, 2, 2, 2, 2, 3]
    assert index[date(2026, 2, 18)] - index[date(2026, 2, 13)] == 1  # un día hábil de distancia


def test_business_day_index_covers_december_holidays():
    index = dict(business_day_index(date(2025, 12, 23), date(2026, 1, 2)))
    assert index[date(2025, 12, 26)] - index[date(2025, 12, 24)] == 1  # Navidad no es hábil
    assert index[date(2026, 1, 2)] - index[date(2025, 12, 31)] == 1  # Año Nuevo no es hábil


@pytest.mark.parametrize(
    "gap, expected",
    [(0, PRICE_EXACT), (1, PRICE_IMPUTED), (IMPUTATION_MAX_BUSINESS_DAYS, PRICE_IMPUTED),
     (IMPUTATION_MAX_BUSINESS_DAYS + 1, PRICE_MISSING), (None, PRICE_MISSING)],
)
def test_classify_price(gap, expected):
    assert classify_price(gap) == expected


def test_price_deviation_pct():
    assert price_deviation_pct(101.0, 100.0) == pytest.approx(1.0)
    assert price_deviation_pct(99.0, 100.0) == pytest.approx(-1.0)
    assert price_deviation_pct(10.0, None) is None


# --- Normalización por fuente (Spark local, opcional) ------------------------------------------

YAHOO_SCHEMA = (
    "simbolo STRING, ticker STRING, date DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, "
    "adj_close DOUBLE, volume DOUBLE, dividends DOUBLE, stock_splits DOUBLE, _extracted_at TIMESTAMP, _source_file STRING"
)
DATA912_SCHEMA = (
    "simbolo STRING, ticker STRING, date DATE, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v DOUBLE, dr DOUBLE, sa DOUBLE, "
    "_extracted_at TIMESTAMP, _source_file STRING"
)
T1, T2 = datetime(2026, 10, 1), datetime(2026, 10, 16)


def _yahoo_row(simbolo, day, close, split=0.0, extracted=T2):
    return (simbolo, f"{simbolo}.BA", day, close, close, close, close, close, 100.0, 0.0, split, extracted, "f")


def test_conform_yahoo_reverts_later_splits(spark):
    from iol_pipeline.silver_cotizaciones import conform_yahoo

    df = spark.createDataFrame([
        _yahoo_row("SPYD", date(2026, 1, 5), 11.78),               # antes de ambos splits: x3 x2
        _yahoo_row("SPYD", date(2026, 5, 29), 12.0, split=3.0),    # día del split: ya en escala nueva -> x2
        _yahoo_row("SPYD", date(2026, 8, 3), 6.1, split=2.0),
        _yahoo_row("SPYD", date(2026, 8, 4), 6.2),                 # después: sin ajuste
        _yahoo_row("GGAL", date(2026, 1, 5), 8515.0),              # sin splits
    ], YAHOO_SCHEMA)
    result = {(r.simbolo, r.fecha): (r.cierre, r.factor_ajuste) for r in conform_yahoo(df).collect()}

    assert result[("SPYD", date(2026, 1, 5))] == (pytest.approx(70.68), 6.0)
    assert result[("SPYD", date(2026, 5, 29))] == (pytest.approx(24.0), 2.0)
    assert result[("SPYD", date(2026, 8, 3))] == (pytest.approx(6.1), 1.0)
    assert result[("SPYD", date(2026, 8, 4))] == (pytest.approx(6.2), 1.0)
    assert result[("GGAL", date(2026, 1, 5))] == (pytest.approx(8515.0), 1.0)


def test_conform_data912_scales_fixed_income_only(spark):
    from iol_pipeline.silver_cotizaciones import conform_data912

    df = spark.createDataFrame([
        ("AL30", "AL30", date(2026, 1, 5), 1.0, 1.0, 1.0, 100450.0, 1.0, 0.0, 0.0, T2, "f"),
        ("GGAL", "GGAL", date(2026, 1, 5), 1.0, 1.0, 1.0, 8345.0, 1.0, 0.0, 0.0, T2, "f"),
    ], DATA912_SCHEMA)
    types = spark.createDataFrame([("AL30", "BONO"), ("GGAL", "ACCION")], "simbolo STRING, tipo_instrumento STRING")
    result = {r.simbolo: (r.cierre, r.cierre_fuente) for r in conform_data912(df, types).collect()}
    assert result["AL30"] == (pytest.approx(1004.5), 100450.0)
    assert result["GGAL"] == (pytest.approx(8345.0), 8345.0)


def test_latest_extraction_keeps_newest_and_older_symbols(spark):
    from iol_pipeline.silver_cotizaciones import latest_extraction

    df = spark.createDataFrame([
        _yahoo_row("GGAL", date(2026, 1, 5), 100.0, extracted=T1),
        _yahoo_row("GGAL", date(2026, 1, 5), 101.0, extracted=T2),   # gana la más reciente
        _yahoo_row("PAMP", date(2026, 1, 5), 50.0, extracted=T1),    # falló en T2: se conserva
    ], YAHOO_SCHEMA)
    result = {r.simbolo: r.close for r in latest_extraction(df).collect()}
    assert result == {"GGAL": 101.0, "PAMP": 50.0}


def test_build_silver_quotes_prefers_by_priority(spark, tmp_path):
    from iol_pipeline.silver_cotizaciones import build_silver_quotes

    yahoo = spark.createDataFrame([
        _yahoo_row("GGAL", date(2026, 1, 5), 8515.0),
        _yahoo_row("GGAL", date(2026, 1, 6), 8470.0),     # data912 no tiene este día
        _yahoo_row("GGALD", date(2026, 1, 5), 5.9),       # variante MEP: solo Yahoo
    ], YAHOO_SCHEMA)
    data912 = spark.createDataFrame([
        ("GGAL", "GGAL", date(2026, 1, 5), 1.0, 1.0, 1.0, 8510.0, 1.0, 0.0, 0.0, T2, "f"),
    ], DATA912_SCHEMA)
    yahoo.createOrReplaceTempView("yahoo_test")
    data912.createOrReplaceTempView("data912_test")
    types = spark.createDataFrame([("GGAL", "ACCION"), ("GGALD", "ACCION")], "simbolo STRING, tipo_instrumento STRING")

    quotes = build_silver_quotes(spark, {"data912": "data912_test", "yahoo": "yahoo_test"}, types, QUOTE_SOURCES,
                                 datetime(2026, 10, 16))
    preferred = {(r.simbolo, r.fecha): r.fuente for r in quotes.where("es_preferida").collect()}
    assert preferred == {
        ("GGAL", date(2026, 1, 5)): "data912",   # prioridad 1
        ("GGAL", date(2026, 1, 6)): "yahoo",     # respaldo cuando falta la primaria
        ("GGALD", date(2026, 1, 5)): "yahoo",
    }
    assert quotes.count() == 4  # se conservan todas las fuentes, no solo la preferida


# --- Precio de mercado de cada operación (Spark local, opcional) ---------------------------------

QUOTES_SCHEMA = "simbolo STRING, fecha DATE, fuente STRING, cierre DOUBLE, es_preferida BOOLEAN"


def test_match_market_prices_exact_imputed_and_missing(spark):
    from iol_pipeline.silver_cotizaciones import match_market_prices

    quotes = spark.createDataFrame([
        ("GGAL", date(2026, 2, 13), "data912", 100.0, True),   # viernes antes de carnaval
        ("GGAL", date(2026, 2, 18), "data912", 102.0, True),
        ("GGAL", date(2026, 2, 18), "yahoo", 999.0, False),    # no preferida: se ignora
        ("AL30", date(2026, 1, 2), "data912", 1000.0, True),
    ], QUOTES_SCHEMA)
    ops = spark.createDataFrame([
        ("T1", "GGAL", date(2026, 2, 18), 103.02),   # mismo día
        ("T2", "GGAL", date(2026, 2, 19), 101.0),    # imputado del 18 (1 día hábil)
        ("T3", "AL30", date(2026, 1, 9), 1010.0),    # imputado del 02/01 (5 días hábiles)
        ("T4", "AL30", date(2026, 1, 12), 1010.0),   # 6 días hábiles: sin cotización
        ("T5", "NOEXISTE", date(2026, 1, 12), 1.0),  # sin cotizaciones
    ], "id_transaccion STRING, simbolo STRING, fecha_operativa DATE, precio DOUBLE")

    result = {r.id_transaccion: r for r in match_market_prices(spark, ops, quotes).collect()}
    assert (result["T1"].tipo_precio, float(result["T1"].precio_mercado), result["T1"].dias_habiles_desfase) == (PRICE_EXACT, 102.0, 0)
    assert result["T1"].desvio_vs_mercado_pct == pytest.approx(1.0)
    assert (result["T2"].tipo_precio, result["T2"].fecha_precio_mercado, result["T2"].dias_habiles_desfase) == (PRICE_IMPUTED, date(2026, 2, 18), 1)
    assert (result["T3"].tipo_precio, result["T3"].dias_habiles_desfase) == (PRICE_IMPUTED, 5)
    assert (result["T4"].tipo_precio, result["T4"].precio_mercado, result["T4"].fuente_precio) == (PRICE_MISSING, None, None)
    assert result["T5"].tipo_precio == PRICE_MISSING
    assert len(result) == 5  # una fila por operación, sin duplicar
