-- =============================================================================
-- Capa Bronze: transacciones crudas tal como llegan de la fuente.
-- Se respetan los nombres de columna originales; la limpieza y el renombrado
-- se hacen en Silver.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS workspace.bronze
COMMENT 'Datos crudos ingeridos desde las fuentes, sin transformaciones de negocio';

CREATE VOLUME IF NOT EXISTS workspace.bronze.checkpoints
COMMENT 'Checkpoints de Auto Loader: registro de archivos ya ingeridos (idempotencia)';

CREATE TABLE IF NOT EXISTS workspace.bronze.transacciones (
    -- Columnas de la fuente
    fecha               TIMESTAMP       COMMENT 'Fecha y hora de la transacción (UTC)',
    tipoTran            STRING          COMMENT 'Tipo de transacción (ej. Compra, Venta)',
    id_cliente          STRING          COMMENT 'Identificador del cliente',
    descripcion_titulo  STRING          COMMENT 'Descripción del instrumento operado',
    moneda              STRING          COMMENT 'Moneda de la operación (ISO, ej. ARS, USD)',
    simbolo_titulo      STRING          COMMENT 'Ticker del instrumento',
    cantidad            DECIMAL(20, 6)  COMMENT 'Cantidad de títulos operados',
    precio              DECIMAL(20, 6)  COMMENT 'Precio unitario en la moneda de la operación',
    id_transaccion      STRING          COMMENT 'Identificador único de la transacción en la fuente',
    origen              STRING          COMMENT 'Canal de origen (ej. Sitio Web Desktop, App)',

    -- Metadatos de ingesta
    _rescued_data       STRING          COMMENT 'Valores que no matchearon el schema (Auto Loader)',
    _source_file        STRING          COMMENT 'Ruta del archivo de origen',
    _file_modified_at   TIMESTAMP       COMMENT 'Fecha de modificación del archivo de origen',
    _ingested_at        TIMESTAMP       COMMENT 'Momento en que el registro se cargó en bronze',
    _ingestion_date     DATE            GENERATED ALWAYS AS (CAST(_ingested_at AS DATE))
                                        COMMENT 'Fecha de ingesta, derivada de _ingested_at'
)
USING DELTA
CLUSTER BY (_ingestion_date)
COMMENT 'Transacciones de clientes (raw) - capa bronze'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality' = 'bronze'
);
