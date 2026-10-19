"""Tests de los clientes de cotizaciones. Sin red: las APIs se simulan."""

from datetime import date, datetime

import pytest

from integrations.finance.base import (
    ERROR_EMPTY,
    ERROR_HTTP,
    ERROR_NOT_FOUND,
    ERROR_PARSE,
    ERROR_UNEXPECTED,
    FetchError,
    FinanceClient,
    QuoteRequest,
)
from integrations.finance.data912 import Data912Client
from integrations.finance.http import RetryableError, retry
from integrations.finance.yahoo import YahooClient, snake_case

START, END = date(2026, 1, 2), date(2026, 3, 13)
NO_SLEEP = {"sleep": lambda seconds: None}


# --- Contrato común ------------------------------------------------------------------

def test_finance_client_is_abstract():
    with pytest.raises(TypeError):
        FinanceClient()


class _FakeClient(FinanceClient):
    source = "fake"

    def __init__(self, behaviour):
        self.behaviour = behaviour

    def supports(self, request):
        return request.tipo_instrumento != "OPCION"

    def ticker_for(self, request):
        return f"{request.simbolo}.X"

    def fetch_symbol(self, request, start, end):
        action = self.behaviour[request.simbolo]
        if isinstance(action, Exception):
            raise action
        return action


def test_fetch_adds_symbol_and_ticker_to_native_rows():
    client = _FakeClient({"OK": [{"date": "2026-01-05", "c": 10.0}]})
    result = client.fetch([QuoteRequest("OK", "ACCION", "ARS")], START, END)
    assert result.records == [{"simbolo": "OK", "ticker": "OK.X", "date": "2026-01-05", "c": 10.0}]


@pytest.mark.parametrize("max_workers", [1, 4])
def test_fetch_isolates_errors_per_symbol(max_workers):
    client = _FakeClient({
        "OK": [{"date": "2026-01-05"}],
        "NOTFOUND": FetchError(ERROR_NOT_FOUND, "no existe"),
        "BOOM": ZeroDivisionError("bug"),
        "VACIO": [],
    })
    requests = [QuoteRequest(s, "ACCION", "ARS") for s in ["OK", "NOTFOUND", "BOOM", "VACIO"]]
    result = client.fetch(requests, START, END, max_workers=max_workers)

    assert [r["simbolo"] for r in result.records] == ["OK"]
    assert [(e.simbolo, e.error_type) for e in result.errors] == [
        ("NOTFOUND", ERROR_NOT_FOUND), ("BOOM", ERROR_UNEXPECTED), ("VACIO", ERROR_EMPTY),
    ]  # en el orden de los pedidos, aun en paralelo


def test_fetch_skips_unsupported_instruments():
    result = _FakeClient({}).fetch([QuoteRequest("GFGC79054F", "OPCION", "ARS")], START, END)
    assert result.records == [] and result.errors == []


# --- Reintentos --------------------------------------------------------------------------

def test_retry_recovers_from_transient_errors():
    calls, delays = [], []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise RetryableError(ERROR_HTTP, "HTTP 429")
        return "ok"

    assert retry(flaky, attempts=4, base_delay=1.0, sleep=delays.append) == "ok"
    assert delays == [1.0, 2.0]  # backoff exponencial


def test_retry_gives_up_with_fetch_error():
    def always_429():
        raise RetryableError(ERROR_HTTP, "HTTP 429")

    with pytest.raises(FetchError) as exc:
        retry(always_429, attempts=3, **NO_SLEEP)
    assert exc.value.error_type == ERROR_HTTP
    assert "3 intentos" in str(exc.value)


def test_retry_respects_retry_after():
    delays = []
    attempts = iter([RetryableError(ERROR_HTTP, "429", retry_after=7.0), None])

    def fn():
        error = next(attempts)
        if error:
            raise error
        return "ok"

    retry(fn, sleep=delays.append)
    assert delays == [7.0]


def test_retry_does_not_retry_definitive_errors():
    calls = []

    def not_found():
        calls.append(1)
        raise FetchError(ERROR_HTTP, "HTTP 404")

    with pytest.raises(FetchError):
        retry(not_found, **NO_SLEEP)
    assert len(calls) == 1


# --- data912 ---------------------------------------------------------------------------

DATA912_SERIES = [
    {"date": "2025-12-30", "o": 1, "h": 1, "l": 1, "c": 1, "v": 1, "dr": 0, "sa": 0},
    {"date": "2026-01-02", "o": 100000.0, "h": 101000.0, "l": 99500.0, "c": 100600.0, "v": 5e7, "dr": 0.01, "sa": 0.2},
    {"date": "2026-03-13", "o": 90000.0, "h": 91000.0, "l": 89000.0, "c": 90500.0, "v": 4e7, "dr": -0.01, "sa": 0.2,
     "campo_nuevo": "x"},
    {"date": "2026-03-16", "o": 1, "h": 1, "l": 1, "c": 1, "v": 1, "dr": 0, "sa": 0},
]


@pytest.mark.parametrize(
    "tipo, endpoint",
    [("BONO", "bonds"), ("LETRA", "bonds"), ("ON", "bonds"), ("ACCION", "stocks"), ("CEDEAR", "cedears")],
)
def test_data912_endpoint_by_instrument_type(tipo, endpoint):
    client = Data912Client(http_get=lambda url: [])
    assert client.url_for(QuoteRequest("X", tipo, "ARS")) == f"https://data912.com/historical/{endpoint}/X"


def test_data912_returns_native_rows_in_range():
    client = Data912Client(http_get=lambda url: DATA912_SERIES)
    result = client.fetch([QuoteRequest("AL30", "BONO", "ARS")], START, END)
    assert [(r["date"], r["c"]) for r in result.records] == [("2026-01-02", 100600.0), ("2026-03-13", 90500.0)]
    # Fidelidad: se conservan todos los campos, incluso los que el schema no conoce.
    assert result.records[1]["campo_nuevo"] == "x"
    assert result.records[0]["sa"] == 0.2


def test_data912_error_payload_is_not_found():
    client = Data912Client(http_get=lambda url: {"Error": "no tengo ese ticker"})
    result = client.fetch([QuoteRequest("S17A6", "LETRA", "ARS")], START, END)
    assert result.errors[0].error_type == ERROR_NOT_FOUND


@pytest.mark.parametrize("payload", ["texto", [{"sin_fecha": 1}]])
def test_data912_unexpected_payload_is_parse_error(payload):
    client = Data912Client(http_get=lambda url: payload)
    result = client.fetch([QuoteRequest("AL30", "BONO", "ARS")], START, END)
    assert result.errors[0].error_type == ERROR_PARSE


def test_data912_does_not_support_options():
    assert not Data912Client().supports(QuoteRequest("GFGC79054F", "OPCION", "ARS"))


# --- Yahoo (yfinance simulado) -----------------------------------------------------------------

class _FakeHistory:
    def __init__(self, rows):
        self.rows = rows

    def iterrows(self):
        return iter(self.rows)


class _FakeYF:
    """Imita `yfinance.Ticker(t).history(...)` con respuestas configurables."""

    def __init__(self, responses):
        self.responses = responses  # ticker -> lista de respuestas/excepciones, en orden
        self.calls = []

    def Ticker(self, ticker):  # noqa: N802 - imita la API de yfinance
        fake = self

        class _T:
            def history(self, **kwargs):
                fake.calls.append((ticker, kwargs))
                response = fake.responses[ticker].pop(0)
                if isinstance(response, Exception):
                    raise response
                return response

        return _T()


def _yf_row(day, close, split=0.0):
    return (datetime(day.year, day.month, day.day),
            {"Open": close, "High": close, "Low": close, "Close": close, "Adj Close": close * 0.99,
             "Volume": 1000, "Dividends": 0.0, "Stock Splits": split})


SPYD_HISTORY = _FakeHistory([
    _yf_row(date(2026, 1, 5), 11.78),
    _yf_row(date(2026, 5, 29), 12.30, split=3.0),
])


def _yahoo(responses):
    return YahooClient(yf_module=_FakeYF(responses), **NO_SLEEP)


def test_yahoo_ticker_uses_byma_suffix():
    assert _yahoo({}).ticker_for(QuoteRequest("SPYD", "CEDEAR", "USD")) == "SPYD.BA"


def test_yahoo_returns_native_rows_with_snake_case_columns():
    client = _yahoo({"SPYD.BA": [SPYD_HISTORY]})
    result = client.fetch([QuoteRequest("SPYD", "CEDEAR", "USD")], START, date(2026, 10, 16))

    first, split_day = result.records
    assert first == {"simbolo": "SPYD", "ticker": "SPYD.BA", "date": "2026-01-05", "open": 11.78, "high": 11.78,
                     "low": 11.78, "close": 11.78, "adj_close": pytest.approx(11.6622), "volume": 1000.0,
                     "dividends": 0.0, "stock_splits": 0.0}
    assert split_day["stock_splits"] == 3.0
    ticker, kwargs = client._yf.calls[0]
    assert kwargs["auto_adjust"] is False and kwargs["end"] == "2026-10-17"  # end exclusivo en yfinance


def test_snake_case():
    assert [snake_case(c) for c in ["Adj Close", "Stock Splits", "Open"]] == ["adj_close", "stock_splits", "open"]


def test_yahoo_retries_on_rate_limit():
    class YFRateLimitError(Exception):
        pass

    client = _yahoo({"GGAL.BA": [YFRateLimitError("Too Many Requests"), SPYD_HISTORY]})
    result = client.fetch([QuoteRequest("GGAL", "ACCION", "ARS")], START, END)
    assert len(result.records) == 2 and result.errors == []


def test_yahoo_missing_ticker_is_not_found():
    class YFTzMissingError(Exception):
        pass

    client = _yahoo({"NOEXISTE.BA": [YFTzMissingError("possibly delisted")]})
    result = client.fetch([QuoteRequest("NOEXISTE", "ACCION", "ARS")], START, END)
    assert result.errors[0].error_type == ERROR_NOT_FOUND


def test_yahoo_does_not_support_fixed_income():
    assert not _yahoo({}).supports(QuoteRequest("AL30", "BONO", "ARS"))


# --- Dirección de dependencias -------------------------------------------------------------

def test_integrations_do_not_depend_on_the_pipeline():
    """Los clientes son genéricos: `iol_pipeline` usa `integrations`, nunca al revés (DT-018)."""
    from pathlib import Path

    package = Path(__file__).resolve().parents[1] / "src" / "integrations"
    offenders = [p.name for p in package.rglob("*.py") if "iol_pipeline" in p.read_text()]
    assert offenders == []
