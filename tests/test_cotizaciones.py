"""Tests del registro de fuentes y de la extracción a landing. Sin red ni Spark."""

import json
import re
from datetime import date, datetime
from pathlib import Path

import pytest

from iol_pipeline.cotizaciones import (
    extract_source,
    extracted_on,
    extraction_window,
    landing_file_name,
    write_jsonl,
)
from integrations.finance.base import ERROR_NOT_FOUND, FetchError, FinanceClient, QuoteRequest
from iol_pipeline.sources import QUOTE_SOURCES, QuoteSource

REPO_ROOT = Path(__file__).resolve().parents[1]
QUOTES_DDL = REPO_ROOT / "setup" / "06_cotizaciones.sql"
INGESTION_METADATA = ["_rescued_data", "_source_file", "_file_modified_at", "_ingested_at"]
EXTRACTED_AT = datetime(2026, 10, 16, 12, 30, 0)


# --- Registro de fuentes -------------------------------------------------------------

def test_source_names_and_tables_are_unique():
    assert len({s.name for s in QUOTE_SOURCES}) == len(QUOTE_SOURCES)
    assert len({s.bronze_table for s in QUOTE_SOURCES}) == len(QUOTE_SOURCES)


def test_registry_can_be_imported_without_optional_dependencies():
    """Los clientes se instancian recién al usarse (yfinance es opcional)."""
    assert all(callable(s.client_factory) for s in QUOTE_SOURCES)


def test_every_instrument_type_with_a_source_has_a_primary():
    best = {}
    for source in QUOTE_SOURCES:
        for tipo, priority in source.priority.items():
            best[tipo] = min(priority, best.get(tipo, priority))
    assert set(best.values()) == {1}


def _ddl_columns(table: str) -> list[tuple[str, str]]:
    body = QUOTES_DDL.read_text().split(f"CREATE TABLE IF NOT EXISTS workspace.bronze.{table}", 1)[1]
    body = body.split("(", 1)[1].split("\n)", 1)[0]
    return re.findall(r"^\s+([a-zA-Z_]\w*)\s+([A-Z]+)\s", body, re.MULTILINE)


@pytest.mark.parametrize("source", QUOTE_SOURCES, ids=lambda s: s.name)
def test_source_bronze_columns_match_ddl(source):
    ddl = _ddl_columns(source.bronze_table)
    assert ddl[: len(source.bronze_columns)] == source.bronze_columns
    assert [name for name, _ in ddl[len(source.bronze_columns):]] == INGESTION_METADATA


def test_requests_for_filters_by_priority():
    source = QuoteSource("x", lambda: None, "t", [], {"BONO": 1})
    requests = [QuoteRequest("AL30", "BONO", "ARS"), QuoteRequest("GGAL", "ACCION", "ARS")]
    assert source.requests_for(requests) == [requests[0]]


# --- Extracción ------------------------------------------------------------------------

def test_extraction_window_includes_lookback_and_today():
    assert extraction_window(date(2026, 1, 2), date(2026, 10, 16)) == (date(2025, 12, 18), date(2026, 10, 16))


def test_landing_file_name_and_detection():
    name = landing_file_name(EXTRACTED_AT)
    assert name == "extraccion_20261016T123000.jsonl"
    assert extracted_on([name], date(2026, 10, 16))
    assert not extracted_on([name], date(2026, 10, 17))


def test_write_jsonl(tmp_path):
    path = tmp_path / "data912" / "x.jsonl"
    assert write_jsonl(str(path), [{"simbolo": "AL30", "c": 1.5}, {"simbolo": "GD30"}], EXTRACTED_AT) == 2
    lines = [json.loads(line) for line in path.read_text().splitlines()]
    assert lines[0] == {"simbolo": "AL30", "c": 1.5, "_extracted_at": "2026-10-16T12:30:00"}


class _StubClient(FinanceClient):
    source = "stub"
    calls = 0

    def supports(self, request):
        return True

    def ticker_for(self, request):
        return request.simbolo

    def fetch_symbol(self, request, start, end):
        _StubClient.calls += 1
        if request.simbolo == "FALTA":
            raise FetchError(ERROR_NOT_FOUND, "no existe")
        return [{"date": "2026-01-05", "c": 1.0}]


STUB_SOURCE = QuoteSource("stub", _StubClient, "stub_cotizaciones", [], {"BONO": 1})
REQUESTS = [QuoteRequest("AL30", "BONO", "ARS"), QuoteRequest("FALTA", "BONO", "ARS"),
            QuoteRequest("GGAL", "ACCION", "ARS")]


def test_extract_source_writes_landing_file_and_returns_errors(tmp_path):
    summary, errors = extract_source(STUB_SOURCE, REQUESTS, date(2026, 1, 1), date(2026, 3, 31),
                                     str(tmp_path / "stub"), EXTRACTED_AT)
    assert (summary.pedidos, summary.simbolos_con_datos, summary.registros, summary.errores) == (2, 1, 1, 1)
    assert Path(summary.archivo).name == "extraccion_20261016T123000.jsonl"
    assert errors[0]["simbolo"] == "FALTA" and errors[0]["error_type"] == ERROR_NOT_FOUND


def test_extract_source_skips_if_already_extracted_today(tmp_path):
    landing = tmp_path / "stub"
    extract_source(STUB_SOURCE, REQUESTS, date(2026, 1, 1), date(2026, 3, 31), str(landing), EXTRACTED_AT)
    calls = _StubClient.calls

    later = datetime(2026, 10, 16, 18, 0, 0)
    summary, _ = extract_source(STUB_SOURCE, REQUESTS, date(2026, 1, 1), date(2026, 3, 31), str(landing), later)
    assert summary.omitida and _StubClient.calls == calls  # no se llamó a la API

    forced, _ = extract_source(STUB_SOURCE, REQUESTS, date(2026, 1, 1), date(2026, 3, 31), str(landing), later,
                               force=True)
    assert not forced.omitida and len(list(landing.iterdir())) == 2
