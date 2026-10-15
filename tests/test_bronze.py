"""Tests de la capa Bronze. Corren sin cluster ni Spark: `pytest tests/`."""

import re
from pathlib import Path

import pytest

from iol_pipeline.bronze import (
    STATUS_FAIL,
    STATUS_OK,
    STATUS_WARN,
    TRANSACCIONES_COLUMNS,
    evaluate_ingestion,
    to_ddl_schema,
)
from iol_pipeline.config import PipelineConfig

REPO_ROOT = Path(__file__).resolve().parents[1]
BRONZE_DDL = REPO_ROOT / "setup" / "02_bronze.sql"


# --- Schema ------------------------------------------------------------------

def _ddl_source_columns() -> list[tuple[str, str]]:
    """Columnas de la fuente declaradas en el DDL (las que no empiezan con `_`)."""
    body = BRONZE_DDL.read_text().split("CREATE TABLE", 1)[1].split("(", 1)[1]
    pattern = re.compile(r"^\s+([a-zA-Z]\w*)\s+([A-Z]+(?:\(\s*\d+\s*,\s*\d+\s*\))?)", re.MULTILINE)
    return [(name, re.sub(r"\s+", "", dtype)) for name, dtype in pattern.findall(body)]


def test_schema_matches_bronze_ddl():
    """El schema de Auto Loader y el DDL de la tabla no deben divergir."""
    assert TRANSACCIONES_COLUMNS == _ddl_source_columns()


def test_schema_matches_csv_header_order():
    header = "fecha,tipoTran,id_cliente,descripcion_titulo,moneda,simbolo_titulo,cantidad,precio,id_transaccion,origen"
    assert [name for name, _ in TRANSACCIONES_COLUMNS] == header.split(",")


def test_amounts_are_decimal_not_float():
    """Montos financieros sin errores de redondeo de punto flotante."""
    types = dict(TRANSACCIONES_COLUMNS)
    assert types["cantidad"].startswith("DECIMAL")
    assert types["precio"].startswith("DECIMAL")


def test_to_ddl_schema():
    assert to_ddl_schema([("a", "STRING"), ("b", "DECIMAL(20,6)")]) == "a STRING, b DECIMAL(20,6)"


# --- Evaluación de la ingesta -------------------------------------------------

@pytest.mark.parametrize(
    "files, source_rows, written, rescued, expected",
    [
        (1, 100_000, 100_000, 0, STATUS_OK),     # carga íntegra
        (0, 0, 0, 0, STATUS_OK),                 # re-ejecución sin archivos nuevos
        (1, 100_000, 100_000, 12, STATUS_WARN),  # íntegra, con valores rescatados
        (1, 100_000, 99_990, 0, STATUS_FAIL),    # se perdieron filas
        (1, 100_000, 200_000, 0, STATUS_FAIL),   # se duplicaron filas
    ],
)
def test_evaluate_ingestion_status(files, source_rows, written, rescued, expected):
    result = evaluate_ingestion("t", files, source_rows, written, rescued)
    assert result.status == expected


def test_evaluate_ingestion_mismatch_takes_precedence_over_rescued():
    assert evaluate_ingestion("t", 1, 10, 9, 5).status == STATUS_FAIL


# --- Configuración -------------------------------------------------------------

def test_config_paths():
    cfg = PipelineConfig(catalog="dev")
    assert cfg.landing_path("transacciones") == "/Volumes/dev/landing/raw_files/transacciones"
    assert cfg.bronze_table("transacciones") == "dev.bronze.transacciones"
    assert cfg.checkpoint_path("transacciones") == "/Volumes/dev/bronze/checkpoints/transacciones/checkpoint"
