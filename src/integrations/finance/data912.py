"""Cliente de data912 (https://data912.com): datos históricos del mercado argentino.

- Precios **sin ajustar** por splits: sirven tal cual para comparar con lo operado.
- La renta fija (bonos, letras, ONs) cotiza **cada 100 de valor nominal**; la
  normalización a precio por 1 VN se hace en Silver.
- Incluye variantes MEP y Cable de bonos (AL30D, AL30C). No tiene variantes
  MEP de acciones ni CEDEARs, ni letras u opciones.
- Es un servicio comunitario sin SLA: se documenta como riesgo de disponibilidad.

Un endpoint por tipo de instrumento, que devuelve la serie completa del ticker:
    GET /historical/{bonds|stocks|cedears}/{ticker}
    -> [{"date": "2026-01-05", "o": ..., "h": ..., "l": ..., "c": ..., "v": ..., "dr": ..., "sa": ...}, ...]
Si el ticker no existe responde 200 con {"Error": "..."}.

Las filas se devuelven tal cual (todas sus claves). Si la fuente agrega campos
nuevos, Auto Loader los guarda en `_rescued_data` de Bronze.
"""

from datetime import date
from typing import Callable

from integrations.finance.base import (
    ERROR_NOT_FOUND,
    ERROR_PARSE,
    FetchError,
    FinanceClient,
    QuoteRequest,
)
from integrations.finance.http import get_json

BASE_URL = "https://data912.com/historical"

ENDPOINTS = {
    "BONO": "bonds",
    "LETRA": "bonds",
    "ON": "bonds",
    "ACCION": "stocks",
    "CEDEAR": "cedears",
}

# Schema de bronze.data912_cotizaciones (campos nativos de la fuente).
# Debe coincidir con setup/06_cotizaciones.sql (lo verifica tests/).
BRONZE_COLUMNS: list[tuple[str, str]] = [
    ("simbolo", "STRING"),
    ("ticker", "STRING"),
    ("date", "DATE"),
    ("o", "DOUBLE"),
    ("h", "DOUBLE"),
    ("l", "DOUBLE"),
    ("c", "DOUBLE"),
    ("v", "DOUBLE"),
    ("dr", "DOUBLE"),
    ("sa", "DOUBLE"),
    ("_extracted_at", "TIMESTAMP"),
]


class Data912Client(FinanceClient):
    source = "data912"

    def __init__(self, http_get: Callable[[str], object] = get_json, base_url: str = BASE_URL):
        self._get = http_get
        self._base_url = base_url

    def supports(self, request: QuoteRequest) -> bool:
        return request.tipo_instrumento in ENDPOINTS

    def ticker_for(self, request: QuoteRequest) -> str:
        return request.simbolo

    def url_for(self, request: QuoteRequest) -> str:
        return f"{self._base_url}/{ENDPOINTS[request.tipo_instrumento]}/{self.ticker_for(request)}"

    def fetch_symbol(self, request: QuoteRequest, start: date, end: date) -> list[dict]:
        payload = self._get(self.url_for(request))
        if isinstance(payload, dict) and "Error" in payload:
            raise FetchError(ERROR_NOT_FOUND, str(payload["Error"]))
        if not isinstance(payload, list):
            raise FetchError(ERROR_PARSE, f"Se esperaba una lista y llegó {type(payload).__name__}")

        rows = []
        for row in payload:
            try:
                day = date.fromisoformat(row["date"])
            except (KeyError, TypeError, ValueError) as e:
                raise FetchError(ERROR_PARSE, f"Fila sin fecha válida: {row!r}") from e
            if start <= day <= end:
                rows.append(dict(row))
        return rows
