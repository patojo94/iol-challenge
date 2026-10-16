"""Cliente de Yahoo Finance (vía la librería `yfinance`).

- Cubre acciones y CEDEARs de BYMA con sufijo `.BA`, en ARS y en MEP (GGAL.BA,
  SPYD.BA). No tiene renta fija ni variantes Cable.
- **Ajusta retroactivamente los precios por splits**, incluso con
  `auto_adjust=False` (SPY 3:1 en mayo 2026 → los cierres de enero aparecen
  divididos por 3). Silver revierte el ajuste con la columna `stock_splits`,
  así que hay que pedir el rango **hasta hoy** para conocer todos los splits
  posteriores al período analizado.
- Ante pedidos masivos responde 429: se reintenta con backoff.

Las filas son las de `Ticker.history(actions=True)` con sus nombres en
snake_case (`Adj Close` → `adj_close`): Delta no admite espacios en nombres
de columna sin column mapping. Es el único cambio sobre el dato nativo.

`yfinance` se importa recién al crear el cliente, para que el resto del
paquete y sus tests no lo requieran.
"""

import re
from datetime import date, timedelta

from integrations.finance.base import (
    ERROR_HTTP,
    ERROR_NOT_FOUND,
    FetchError,
    FinanceClient,
    QuoteRequest,
)
from integrations.finance.http import RetryableError, retry

SUPPORTED_TYPES = ("ACCION", "CEDEAR")
TICKER_SUFFIX = ".BA"

# Excepciones de yfinance que indican que el ticker no existe o no tiene datos.
NOT_FOUND_ERRORS = ("YFPricesMissingError", "YFTzMissingError", "YFTickerMissingError")

# Schema de bronze.yahoo_cotizaciones (campos nativos de la fuente).
# Debe coincidir con setup/06_cotizaciones.sql (lo verifica tests/).
BRONZE_COLUMNS: list[tuple[str, str]] = [
    ("simbolo", "STRING"),
    ("ticker", "STRING"),
    ("date", "DATE"),
    ("open", "DOUBLE"),
    ("high", "DOUBLE"),
    ("low", "DOUBLE"),
    ("close", "DOUBLE"),
    ("adj_close", "DOUBLE"),
    ("volume", "DOUBLE"),
    ("dividends", "DOUBLE"),
    ("stock_splits", "DOUBLE"),
    ("_extracted_at", "TIMESTAMP"),
]


def _is_rate_limit(error: Exception) -> bool:
    return "RateLimit" in type(error).__name__ or "429" in str(error) or "Too Many Requests" in str(error)


def snake_case(name: str) -> str:
    return re.sub(r"[^0-9a-zA-Z]+", "_", str(name)).strip("_").lower()


def _value(value):
    """Convierte tipos de pandas/numpy a tipos JSON (NaN -> None)."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return value
    return None if number != number else number


class YahooClient(FinanceClient):
    source = "yahoo"

    def __init__(self, yf_module=None, **retry_kwargs):
        if yf_module is None:
            import yfinance as yf_module  # noqa: PLC0415 - dependencia opcional
        self._yf = yf_module
        self._retry_kwargs = retry_kwargs

    def supports(self, request: QuoteRequest) -> bool:
        return request.tipo_instrumento in SUPPORTED_TYPES

    def ticker_for(self, request: QuoteRequest) -> str:
        return f"{request.simbolo}{TICKER_SUFFIX}"

    def fetch_symbol(self, request: QuoteRequest, start: date, end: date) -> list[dict]:
        ticker = self.ticker_for(request)

        def _once():
            try:
                # `end` es exclusivo en yfinance: se suma un día para incluirlo.
                return self._yf.Ticker(ticker).history(
                    start=start.isoformat(), end=(end + timedelta(days=1)).isoformat(),
                    auto_adjust=False, actions=True, raise_errors=True,
                )
            except Exception as e:
                if _is_rate_limit(e):
                    raise RetryableError(ERROR_HTTP, f"Rate limit de Yahoo: {e}") from e
                if type(e).__name__ in NOT_FOUND_ERRORS:
                    raise FetchError(ERROR_NOT_FOUND, f"{type(e).__name__}: {e}") from e
                raise

        history = retry(_once, **self._retry_kwargs)
        return [
            {"date": ts.date().isoformat(), **{snake_case(k): _value(v) for k, v in row.items()}}
            for ts, row in history.iterrows()
        ]
