"""Registro de fuentes de cotizaciones (DT-019).

El pipeline recorre `QUOTE_SOURCES`: para sumar una fuente, se agrega un
`QuoteSource` con su cliente, su tabla Bronze (y su DDL en
setup/06_cotizaciones.sql) y sus prioridades por tipo de instrumento.

La prioridad decide en Silver qué precio se usa cuando varias fuentes cotizan
el mismo símbolo y día: gana el número más bajo. Una fuente sin prioridad
para un tipo de instrumento no se consulta para ese tipo.
"""

from dataclasses import dataclass
from typing import Callable

from integrations.finance import data912, yahoo
from integrations.finance.base import FinanceClient, QuoteRequest


@dataclass(frozen=True)
class QuoteSource:
    name: str
    client_factory: Callable[[], FinanceClient]   # se instancia recién al usarla
    bronze_table: str                             # nombre de la tabla en el schema bronze
    bronze_columns: list[tuple[str, str]]         # campos nativos de la fuente
    priority: dict[str, int]                      # tipo de instrumento -> prioridad (1 = primaria)
    max_workers: int = 1                          # pedidos concurrentes a la API

    def requests_for(self, requests: list[QuoteRequest]) -> list[QuoteRequest]:
        return [r for r in requests if r.tipo_instrumento in self.priority]


QUOTE_SOURCES: list[QuoteSource] = [
    QuoteSource(
        name="data912",
        client_factory=data912.Data912Client,
        bronze_table="data912_cotizaciones",
        bronze_columns=data912.BRONZE_COLUMNS,
        # Primaria donde tiene cobertura: precios sin ajustar por splits.
        priority={"BONO": 1, "LETRA": 1, "ON": 1, "ACCION": 1, "CEDEAR": 1},
        max_workers=4,
    ),
    QuoteSource(
        name="yahoo",
        client_factory=yahoo.YahooClient,
        bronze_table="yahoo_cotizaciones",
        bronze_columns=yahoo.BRONZE_COLUMNS,
        # Respaldo en acciones y CEDEARs; única fuente para sus variantes MEP.
        priority={"ACCION": 2, "CEDEAR": 2},
        max_workers=2,
    ),
]
