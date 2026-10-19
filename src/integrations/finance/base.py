"""Contrato común de los clientes de cotizaciones (DT-018).

Los clientes devuelven las filas **en el formato nativo de cada fuente**, sin
normalizar: cada fuente tiene su propia tabla Bronze (DT-019) y la conversión
al modelo común se hace en Silver. Así una regla nueva se reprocesa sin
volver a llamar a las APIs.

Un cliente nunca lanza excepciones por un símbolo: los errores se devuelven
en `FetchResult.errors` y la corrida sigue con el resto (degradación elegante).
"""

from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from datetime import date

# Tipos de error, para la tabla de cobertura y el monitoreo.
ERROR_NOT_FOUND = "NOT_FOUND"      # la fuente no tiene el ticker
ERROR_EMPTY = "EMPTY"              # el ticker existe pero no hay datos en el rango
ERROR_HTTP = "HTTP"                # error HTTP tras agotar los reintentos
ERROR_TIMEOUT = "TIMEOUT"          # sin respuesta tras agotar los reintentos
ERROR_PARSE = "PARSE"              # respuesta con formato inesperado
ERROR_UNEXPECTED = "UNEXPECTED"


class FetchError(Exception):
    """Error de un cliente al traer un símbolo, con su tipo."""

    def __init__(self, error_type: str, message: str):
        super().__init__(message)
        self.error_type = error_type


@dataclass(frozen=True)
class QuoteRequest:
    """Qué cotización se pide. El tipo de instrumento permite a cada fuente
    resolver el endpoint o el ticker que corresponde."""

    simbolo: str
    tipo_instrumento: str
    moneda: str


@dataclass(frozen=True)
class SymbolError:
    simbolo: str
    ticker: str
    fuente: str
    error_type: str
    message: str


@dataclass
class FetchResult:
    # Filas en el formato nativo de la fuente. `fetch` les agrega `simbolo`
    # (el del dataset) y `ticker` (el de la fuente).
    records: list[dict] = field(default_factory=list)
    errors: list[SymbolError] = field(default_factory=list)

    def extend(self, other: "FetchResult") -> None:
        self.records.extend(other.records)
        self.errors.extend(other.errors)

    def errors_as_dicts(self) -> list[dict]:
        return [asdict(e) for e in self.errors]


class FinanceClient(ABC):
    """Cliente de una fuente de cotizaciones.

    Las subclases definen `source`, `supports`, `ticker_for` y `fetch_symbol`.
    `fetch` es común: recorre los pedidos (en paralelo si `max_workers > 1`)
    y aísla los errores por símbolo.
    """

    source: str

    @abstractmethod
    def supports(self, request: QuoteRequest) -> bool:
        """Si la fuente puede cotizar ese tipo de instrumento."""

    @abstractmethod
    def ticker_for(self, request: QuoteRequest) -> str:
        """Identificador del instrumento en la fuente."""

    @abstractmethod
    def fetch_symbol(self, request: QuoteRequest, start: date, end: date) -> list[dict]:
        """Filas nativas de la fuente para un símbolo en [start, end].
        Lanza `FetchError` si falla."""

    def _fetch_one(self, request: QuoteRequest, start: date, end: date) -> FetchResult:
        ticker = self.ticker_for(request)
        try:
            rows = self.fetch_symbol(request, start, end)
            if not rows:
                raise FetchError(ERROR_EMPTY, f"Sin cotizaciones entre {start} y {end}")
            return FetchResult(records=[{"simbolo": request.simbolo, "ticker": ticker, **row} for row in rows])
        except FetchError as e:
            error = SymbolError(request.simbolo, ticker, self.source, e.error_type, str(e))
        except Exception as e:  # noqa: BLE001 - ningún símbolo puede cortar la corrida
            error = SymbolError(request.simbolo, ticker, self.source, ERROR_UNEXPECTED, f"{type(e).__name__}: {e}")
        return FetchResult(errors=[error])

    def fetch(self, requests: list[QuoteRequest], start: date, end: date, max_workers: int = 1) -> FetchResult:
        supported = [r for r in requests if self.supports(r)]
        result = FetchResult()
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            # `map` conserva el orden de los pedidos: el resultado es determinista.
            for partial in pool.map(lambda r: self._fetch_one(r, start, end), supported):
                result.extend(partial)
        return result
