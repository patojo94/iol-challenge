-- =============================================================================
-- Cotizaciones de mercado (Task 04, DT-019).
--
-- Una tabla Bronze por fuente, con sus campos nativos: Bronze está alineado
-- a la fuente y Silver unifica. Para sumar una fuente: agregar su tabla acá y
-- su QuoteSource en src/iol_pipeline/sources.py.
--
-- Las respuestas crudas aterrizan en
--   /Volumes/workspace/landing/raw_files/cotizaciones/{fuente}/extraccion_{ts}.jsonl
-- y entran con Auto Loader (idempotente por archivo, como transacciones).
-- =============================================================================

CREATE TABLE IF NOT EXISTS workspace.bronze.data912_cotizaciones (
    -- Campos de la fuente (https://data912.com/historical/...)
    simbolo             STRING          COMMENT 'Símbolo del dataset',
    ticker              STRING          COMMENT 'Ticker en data912',
    date                DATE            COMMENT 'Fecha de la rueda',
    o                   DOUBLE          COMMENT 'Apertura (renta fija: cada 100 VN)',
    h                   DOUBLE          COMMENT 'Máximo',
    l                   DOUBLE          COMMENT 'Mínimo',
    c                   DOUBLE          COMMENT 'Cierre (sin ajustar por splits)',
    v                   DOUBLE          COMMENT 'Volumen',
    dr                  DOUBLE          COMMENT 'Variación diaria (campo de la fuente)',
    sa                  DOUBLE          COMMENT 'Campo de la fuente (sin documentación oficial)',
    _extracted_at       TIMESTAMP       COMMENT 'Momento de la extracción de la API',

    -- Metadatos de ingesta
    _rescued_data       STRING          COMMENT 'Campos nuevos o que no matchearon el schema (Auto Loader)',
    _source_file        STRING          COMMENT 'Archivo de landing de origen',
    _file_modified_at   TIMESTAMP       COMMENT 'Fecha de modificación del archivo de origen',
    _ingested_at        TIMESTAMP       COMMENT 'Momento en que el registro se cargó en bronze'
)
USING DELTA
CLUSTER BY (simbolo)
COMMENT 'Cotizaciones diarias de data912, tal como las publica la fuente'
TBLPROPERTIES ('quality' = 'bronze');

CREATE TABLE IF NOT EXISTS workspace.bronze.yahoo_cotizaciones (
    -- Campos de la fuente (yfinance Ticker.history(actions=True), en snake_case)
    simbolo             STRING          COMMENT 'Símbolo del dataset',
    ticker              STRING          COMMENT 'Ticker en Yahoo (sufijo .BA)',
    date                DATE            COMMENT 'Fecha de la rueda',
    open                DOUBLE          COMMENT 'Apertura (ajustada por splits posteriores)',
    high                DOUBLE          COMMENT 'Máximo (ajustado por splits posteriores)',
    low                 DOUBLE          COMMENT 'Mínimo (ajustado por splits posteriores)',
    close               DOUBLE          COMMENT 'Cierre (ajustado por splits posteriores)',
    adj_close           DOUBLE          COMMENT 'Cierre ajustado por splits y dividendos',
    volume              DOUBLE          COMMENT 'Volumen',
    dividends           DOUBLE          COMMENT 'Dividendo pagado ese día',
    stock_splits        DOUBLE          COMMENT 'Factor de split ese día (0 si no hubo)',
    _extracted_at       TIMESTAMP       COMMENT 'Momento de la extracción de la API',

    -- Metadatos de ingesta
    _rescued_data       STRING          COMMENT 'Campos nuevos o que no matchearon el schema (Auto Loader)',
    _source_file        STRING          COMMENT 'Archivo de landing de origen',
    _file_modified_at   TIMESTAMP       COMMENT 'Fecha de modificación del archivo de origen',
    _ingested_at        TIMESTAMP       COMMENT 'Momento en que el registro se cargó en bronze'
)
USING DELTA
CLUSTER BY (simbolo)
COMMENT 'Cotizaciones diarias de Yahoo Finance, tal como las publica la fuente'
TBLPROPERTIES ('quality' = 'bronze');

CREATE TABLE IF NOT EXISTS workspace.bronze.errores_api (
    simbolo             STRING          COMMENT 'Símbolo pedido',
    ticker              STRING          COMMENT 'Ticker en la fuente',
    fuente              STRING          COMMENT 'Fuente consultada',
    error_type          STRING          COMMENT 'NOT_FOUND, EMPTY, HTTP, TIMEOUT, PARSE o UNEXPECTED',
    message             STRING          COMMENT 'Detalle del error',
    _extracted_at       TIMESTAMP       COMMENT 'Momento de la extracción'
)
USING DELTA
COMMENT 'Registro operativo de errores de las APIs de cotizaciones (todas las fuentes)'
TBLPROPERTIES ('quality' = 'bronze');
