"""HTTP con reintentos, compartido por los clientes.

Reintenta ante errores transitorios (429, 5xx, timeouts) con backoff
exponencial; respeta `Retry-After` si la fuente lo envía. Los errores
definitivos (404, otros 4xx) no se reintentan.
"""

import json
import socket
import time
import urllib.error
import urllib.request
from typing import Callable, TypeVar

from integrations.finance.base import ERROR_HTTP, ERROR_PARSE, ERROR_TIMEOUT, FetchError

T = TypeVar("T")

RETRYABLE_STATUS = {429, 500, 502, 503, 504}
USER_AGENT = "Mozilla/5.0 (iol-challenge data pipeline)"


class RetryableError(Exception):
    """Error transitorio: vale la pena reintentar."""

    def __init__(self, error_type: str, message: str, retry_after: float | None = None):
        super().__init__(message)
        self.error_type = error_type
        self.retry_after = retry_after


def retry(
    fn: Callable[[], T],
    attempts: int = 4,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Ejecuta `fn` reintentando ante `RetryableError` con backoff exponencial
    (1s, 2s, 4s...). Agotados los intentos, lanza `FetchError`."""
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except RetryableError as e:
            if attempt == attempts:
                raise FetchError(e.error_type, f"{e} (tras {attempts} intentos)") from e
            delay = e.retry_after if e.retry_after is not None else base_delay * 2 ** (attempt - 1)
            sleep(min(delay, max_delay))
    raise AssertionError("unreachable")


def _retry_after_seconds(error: urllib.error.HTTPError) -> float | None:
    value = error.headers.get("Retry-After") if error.headers else None
    try:
        return float(value) if value is not None else None
    except ValueError:
        return None


def get_json(url: str, timeout: float = 20.0, **retry_kwargs):
    """GET que devuelve el JSON parseado, con reintentos ante errores transitorios."""

    def _once():
        request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                body = response.read()
        except urllib.error.HTTPError as e:
            if e.code in RETRYABLE_STATUS:
                raise RetryableError(ERROR_HTTP, f"HTTP {e.code}", _retry_after_seconds(e)) from e
            raise FetchError(ERROR_HTTP, f"HTTP {e.code}") from e
        except (socket.timeout, TimeoutError) as e:
            raise RetryableError(ERROR_TIMEOUT, "Timeout") from e
        except urllib.error.URLError as e:
            raise RetryableError(ERROR_HTTP, f"Error de conexión: {e.reason}") from e
        try:
            return json.loads(body)
        except ValueError as e:
            raise FetchError(ERROR_PARSE, "La respuesta no es JSON válido") from e

    return retry(_once, **retry_kwargs)
