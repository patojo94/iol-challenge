"""Tests de la capa Gold. Los de lógica pura corren sin Spark: `pytest tests/`."""

import re
from datetime import date, datetime
from pathlib import Path

import pytest

from iol_pipeline.gold import (
    DIM_CANAL_COLUMNS,
    DIM_CLIENTE_COLUMNS,
    DIM_FECHA_COLUMNS,
    DIM_INSTRUMENTO_COLUMNS,
    FACT_COLUMNS,
    SCD_END_DATE,
    STATUS_FAIL,
    STATUS_OK,
    activity_segment,
    date_dimension_rows,
    date_key,
    evaluate_gold,
    mask_client_id,
    monthly_cutoffs,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
GOLD_DDL = REPO_ROOT / "setup" / "05_gold.sql"


# --- Segmentación (DT-007) ----------------------------------------------------------

@pytest.mark.parametrize(
    "operations, segment",
    [(1, "OCASIONAL"), (2, "RECURRENTE"), (4, "RECURRENTE"), (5, "FRECUENTE"),
     (19, "FRECUENTE"), (20, "INTENSIVO"), (676, "INTENSIVO")],
)
def test_activity_segment_thresholds(operations, segment):
    assert activity_segment(operations) == segment


def test_activity_segment_rejects_clients_without_operations():
    with pytest.raises(ValueError):
        activity_segment(0)


# --- Cortes del SCD2 (DT-016) -------------------------------------------------------

def test_monthly_cutoffs_with_incomplete_last_month():
    assert monthly_cutoffs(date(2026, 1, 2), date(2026, 3, 13)) == [
        (date(2026, 1, 1), date(2026, 1, 31)),
        (date(2026, 2, 1), date(2026, 2, 28)),
        (date(2026, 3, 1), date(2026, 3, 13)),
    ]


def test_monthly_cutoffs_single_month():
    assert monthly_cutoffs(date(2026, 2, 3), date(2026, 2, 10)) == [(date(2026, 2, 1), date(2026, 2, 10))]


def test_mask_client_id():
    assert mask_client_id("CLI0000TEST") == "CLI***0TEST"
    assert "0000" not in mask_client_id("CLI0000TEST")


# --- Dimensión fecha --------------------------------------------------------------

def test_date_dimension_rows():
    rows = {r[1]: r for r in date_dimension_rows(date(2026, 2, 13), date(2026, 2, 18))}
    viernes, sabado, carnaval = rows[date(2026, 2, 13)], rows[date(2026, 2, 14)], rows[date(2026, 2, 16)]
    assert viernes[0] == 20260213 and viernes[-1] is True
    assert sabado[8] == "sábado" and sabado[9] is True and sabado[-1] is False
    assert carnaval[10] is True and carnaval[11] == "Carnaval" and carnaval[-1] is False
    assert len(rows) == 6


def test_date_key():
    assert date_key(date(2026, 3, 9)) == 20260309


# --- Evaluación ---------------------------------------------------------------------

NO_ORPHANS = {"fecha_key": 0, "instrumento_key": 0, "canal_key": 0, "cliente_sk": 0}


def test_evaluate_gold_ok():
    assert evaluate_gold(100, 100, NO_ORPHANS, 0, 0).status == STATUS_OK


@pytest.mark.parametrize(
    "fact_rows, orphans, sin_vigente, superpuestas",
    [
        (99, NO_ORPHANS, 0, 0),
        (100, {**NO_ORPHANS, "cliente_sk": 3}, 0, 0),
        (100, NO_ORPHANS, 1, 0),
        (100, NO_ORPHANS, 0, 2),
    ],
)
def test_evaluate_gold_fail(fact_rows, orphans, sin_vigente, superpuestas):
    assert evaluate_gold(100, fact_rows, orphans, sin_vigente, superpuestas).status == STATUS_FAIL


# --- Schema ---------------------------------------------------------------------------

def _ddl_columns(table: str) -> list[str]:
    body = GOLD_DDL.read_text().split(f"CREATE TABLE IF NOT EXISTS workspace.gold.{table}", 1)[1]
    body = body.split("(", 1)[1].split("\n)", 1)[0]
    return re.findall(r"^\s+([a-zA-Z_]\w*)\s+[A-Z]", body, re.MULTILINE)


@pytest.mark.parametrize(
    "table, columns",
    [
        ("dim_fecha", DIM_FECHA_COLUMNS),
        ("dim_canal", DIM_CANAL_COLUMNS),
        ("dim_instrumento", DIM_INSTRUMENTO_COLUMNS),
        ("dim_cliente", DIM_CLIENTE_COLUMNS),
        ("fact_operaciones", FACT_COLUMNS),
    ],
)
def test_schema_matches_gold_ddl(table, columns):
    assert columns == _ddl_columns(table)


# --- SCD Tipo 2 con Delta local (opcional) ------------------------------------------
# Se saltea si PySpark o delta-spark no están instalados.

DIM_CLIENTE_DDL = """
    CREATE OR REPLACE TABLE dim_cliente_test (
        cliente_sk BIGINT, id_cliente STRING, segmento_actividad STRING, canal_principal STRING,
        fecha_primera_operacion DATE, valid_from DATE, valid_to DATE, is_current BOOLEAN,
        _attr_hash STRING, _loaded_at TIMESTAMP
    ) USING DELTA
"""


def _silver(spark, rows):
    return spark.createDataFrame(
        [(c, canal, d, datetime(d.year, d.month, d.day, 15)) for c, canal, d in rows],
        "id_cliente STRING, canal STRING, fecha_operativa DATE, fecha_hora_utc TIMESTAMP",
    )


def test_scd2_versions_history(delta_spark):
    from iol_pipeline.gold import build_dim_cliente, build_fact

    spark = delta_spark
    spark.sql(DIM_CLIENTE_DDL)
    silver = _silver(spark, [
        ("A", "App Mobile", date(2026, 1, 5)),                                                # ene: OCASIONAL
        ("A", "App Mobile", date(2026, 2, 3)), ("A", "App Mobile", date(2026, 2, 4)),          # feb: RECURRENTE
        ("A", "API", date(2026, 3, 2)), ("A", "API", date(2026, 3, 3)), ("A", "API", date(2026, 3, 4)),  # mar: canal API
        ("B", "API", date(2026, 1, 7)),                                                       # sin cambios
        ("C", "App Mobile", date(2026, 3, 9)),                                                # nuevo en marzo
    ])
    cutoffs = monthly_cutoffs(date(2026, 1, 5), date(2026, 3, 13))
    loads = build_dim_cliente(spark, silver, "dim_cliente_test", cutoffs, datetime(2026, 3, 14))

    assert [(l["nuevos"], l["con_cambios"], l["sin_cambios"]) for l in loads] == [(2, 0, 0), (0, 1, 1), (1, 1, 1)]

    dim = spark.table("dim_cliente_test")
    a = [(r.segmento_actividad, r.canal_principal, r.valid_from, r.valid_to, r.is_current)
         for r in dim.where("id_cliente = 'A'").orderBy("valid_from").collect()]
    assert a == [
        ("OCASIONAL", "App Mobile", date(2026, 1, 1), date(2026, 1, 31), False),
        ("RECURRENTE", "App Mobile", date(2026, 2, 1), date(2026, 2, 28), False),
        ("FRECUENTE", "API", date(2026, 3, 1), SCD_END_DATE, True),
    ]
    assert dim.where("id_cliente = 'B'").count() == 1
    assert dim.where("id_cliente = 'C'").first().valid_from == date(2026, 3, 1)

    # Point-in-time: cada operación apunta a la versión vigente en su fecha.
    fact = build_fact(
        silver.selectExpr("*", "'TX' || id_cliente || fecha_operativa AS id_transaccion", "'X' AS simbolo",
                          "CAST(NULL AS TIMESTAMP) AS fecha_hora_local", "'COMPRA' AS tipo_operacion",
                          "'ARS' AS moneda", "1.0 AS cantidad", "1.0 AS precio", "1.0 AS monto",
                          "true AS en_horario_mercado", "'OK' AS dq_status", "array() AS dq_flags"),
        dim, datetime(2026, 3, 14),
    )
    versions = fact.join(dim, "cliente_sk").where("id_cliente = 'A'").groupBy("segmento_actividad").count()
    assert {r[0]: r[1] for r in versions.collect()} == {"OCASIONAL": 1, "RECURRENTE": 2, "FRECUENTE": 3}
    assert fact.where("cliente_sk IS NULL").count() == 0


def test_scd2_reapplying_snapshot_is_idempotent(delta_spark):
    from iol_pipeline.gold import apply_scd2, client_snapshot

    spark = delta_spark
    spark.sql(DIM_CLIENTE_DDL)
    silver = _silver(spark, [("A", "App Mobile", date(2026, 1, 5)), ("A", "API", date(2026, 1, 6))])
    snapshot = client_snapshot(silver, date(2026, 1, 31))
    apply_scd2(spark, "dim_cliente_test", snapshot, date(2026, 1, 1), datetime(2026, 2, 1))
    again = apply_scd2(spark, "dim_cliente_test", snapshot, date(2026, 1, 1), datetime(2026, 2, 1))
    assert (again["nuevos"], again["con_cambios"], again["sin_cambios"]) == (0, 0, 1)
    assert spark.table("dim_cliente_test").count() == 1
