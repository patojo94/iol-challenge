"""Extracción de cotizaciones a landing (Task 04, DT-019).

    silver.transacciones ──▶ pedidos (símbolo, tipo, moneda)
                                  │
              QUOTE_SOURCES ──────┤  por fuente: cliente.fetch(...)
                                  ▼
    landing/raw_files/cotizaciones/{fuente}/extraccion_{ts}.jsonl  ──Auto Loader──▶ bronze.{fuente}_cotizaciones
    bronze.errores_api  ◀── errores por símbolo de todas las fuentes

Cada extracción es un archivo nuevo: Bronze guarda la historia de lo que
respondió cada API y cuándo. Silver se queda con la última extracción.
"""

import json
import os
from dataclasses import asdict, astuple, dataclass
from datetime import date, datetime, timedelta

from integrations.finance.base import QuoteRequest
from iol_pipeline.sources import QuoteSource

# Margen antes del período para poder imputar el precio de una operación con
# un cierre anterior (DT-017: hasta 5 días hábiles ≈ 7 corridos, con holgura).
LOOKBACK_DAYS = 15

ERRORS_COLUMNS = ["simbolo", "ticker", "fuente", "error_type", "message", "_extracted_at"]


def extraction_window(first_operation: date, today: date, lookback_days: int = LOOKBACK_DAYS) -> tuple[date, date]:
    """Desde antes de la primera operación hasta hoy: hasta hoy porque Yahoo
    necesita los splits posteriores al período para revertir su ajuste."""
    return first_operation - timedelta(days=lookback_days), today


def landing_file_name(extracted_at: datetime) -> str:
    return f"extraccion_{extracted_at:%Y%m%dT%H%M%S}.jsonl"


def extracted_on(file_names: list[str], day: date) -> bool:
    """Si ya hay un archivo de extracción de ese día."""
    return any(name.startswith(f"extraccion_{day:%Y%m%d}") for name in file_names)


def write_jsonl(path: str, records: list[dict], extracted_at: datetime) -> int:
    """Escribe una línea JSON por registro, con el momento de extracción."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    stamp = extracted_at.isoformat()
    with open(path, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps({**record, "_extracted_at": stamp}, ensure_ascii=False, default=str) + "\n")
    return len(records)


# Schema explícito para mostrar los resúmenes: si todas las fuentes se omiten,
# `archivo` es NULL en todas las filas y Spark no puede inferir su tipo.
EXTRACTION_SUMMARY_SCHEMA = (
    "fuente STRING, pedidos INT, simbolos_con_datos INT, registros INT, errores INT, "
    "archivo STRING, omitida BOOLEAN, motivo STRING"
)


@dataclass
class ExtractionSummary:
    fuente: str
    pedidos: int
    simbolos_con_datos: int
    registros: int
    errores: int
    archivo: str | None
    omitida: bool
    motivo: str

    def to_dict(self) -> dict:
        return asdict(self)

    def to_row(self) -> tuple:
        """Valores en el orden de EXTRACTION_SUMMARY_SCHEMA."""
        return astuple(self)


def extract_source(
    source: QuoteSource,
    requests: list[QuoteRequest],
    start: date,
    end: date,
    landing_dir: str,
    extracted_at: datetime,
    force: bool = False,
) -> tuple[ExtractionSummary, list[dict]]:
    """Extrae una fuente a su carpeta de landing.

    Si ya se extrajo hoy y no se fuerza, no se llama a la API: re-ejecutar el
    pipeline el mismo día no vuelve a consultar las fuentes.
    Devuelve el resumen y los errores por símbolo (para bronze.errores_api).
    """
    source_requests = source.requests_for(requests)
    existing = os.listdir(landing_dir) if os.path.isdir(landing_dir) else []
    if not force and extracted_on(existing, extracted_at.date()):
        return ExtractionSummary(source.name, len(source_requests), 0, 0, 0, None, True,
                                 f"Ya hay una extracción del {extracted_at.date()}"), []

    client = source.client_factory()
    result = client.fetch(source_requests, start, end, max_workers=source.max_workers)
    path = os.path.join(landing_dir, landing_file_name(extracted_at))
    written = write_jsonl(path, result.records, extracted_at) if result.records else 0

    errors = [{**e, "_extracted_at": extracted_at} for e in result.errors_as_dicts()]
    summary = ExtractionSummary(
        fuente=source.name,
        pedidos=len(source_requests),
        simbolos_con_datos=len({r["simbolo"] for r in result.records}),
        registros=written,
        errores=len(errors),
        archivo=path if written else None,
        omitida=False,
        motivo="",
    )
    return summary, errors


def build_requests(silver_df) -> list[QuoteRequest]:
    """Un pedido por símbolo operado, con su tipo de instrumento y moneda."""
    rows = silver_df.select("simbolo", "tipo_instrumento", "moneda").distinct().orderBy("simbolo").collect()
    return [QuoteRequest(r["simbolo"], r["tipo_instrumento"], r["moneda"]) for r in rows]
