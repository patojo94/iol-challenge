-- =============================================================================
-- Capa Silver: transacciones limpias, tipadas y enriquecidas.
-- Ningún registro se descarta por calidad: los problemas se marcan en
-- dq_flags (DT-003). Solo se eliminan duplicados de id_transaccion, que
-- siguen trazables en bronze.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS workspace.silver
COMMENT 'Datos limpios y enriquecidos, con flags de calidad, listos para modelar';

CREATE TABLE IF NOT EXISTS workspace.silver.ref_feriados (
    fecha               DATE            COMMENT 'Feriado nacional con mercado cerrado',
    descripcion         STRING          COMMENT 'Nombre del feriado'
)
USING DELTA
COMMENT 'Feriados de Argentina usados para calcular días hábiles (DT-004)';

CREATE TABLE IF NOT EXISTS workspace.silver.transacciones (
    -- Identificación y tiempo
    id_transaccion      STRING          COMMENT 'Identificador único de la transacción (sin duplicados)',
    fecha_hora_utc      TIMESTAMP       COMMENT 'Fecha y hora de la transacción en UTC (como llega de la fuente)',
    fecha_hora_local    TIMESTAMP       COMMENT 'Fecha y hora en Argentina (America/Argentina/Buenos_Aires)',
    fecha_local         DATE            COMMENT 'Fecha calendario en Argentina',
    fecha_operativa     DATE            COMMENT 'Día hábil al que se imputa la operación (siguiente hábil si fecha_local no lo es)',
    es_dia_habil        BOOLEAN         COMMENT 'fecha_local es día hábil de mercado',
    en_horario_mercado  BOOLEAN         COMMENT 'La operación ocurrió en día hábil y dentro del horario de BYMA (11 a 17 hs)',

    -- Operación
    id_cliente          STRING          COMMENT 'Identificador del cliente',
    tipo_operacion      STRING          COMMENT 'COMPRA o VENTA',
    canal               STRING          COMMENT 'Canal de origen de la operación',

    -- Instrumento
    simbolo             STRING          COMMENT 'Ticker operado, tal como llega de la fuente',
    simbolo_base        STRING          COMMENT 'Ticker sin sufijo de liquidación (AL30D -> AL30)',
    tipo_liquidacion    STRING          COMMENT 'ARS, MEP (sufijo D) o CABLE (sufijo C)',
    tipo_instrumento    STRING          COMMENT 'CEDEAR, BONO, LETRA, ON, OPCION, WARRANT o ACCION',
    descripcion_titulo  STRING          COMMENT 'Descripción del instrumento (sin espacios extremos)',
    moneda              STRING          COMMENT 'Moneda de la operación (ARS, USD)',

    -- Valores
    cantidad            DECIMAL(20, 6)  COMMENT 'Cantidad de títulos (nominales en renta fija)',
    precio              DECIMAL(20, 6)  COMMENT 'Precio unitario en la moneda de la operación',
    monto               DECIMAL(24, 6)  COMMENT 'cantidad x precio, en la moneda de la operación',
    precio_referencia   DECIMAL(20, 6)  COMMENT 'Mediana del precio del símbolo y moneda en todo el histórico',

    -- Calidad
    dq_flags            ARRAY<STRING>   COMMENT 'Problemas de calidad detectados (vacío si no hay)',
    dq_status           STRING          COMMENT 'Peor severidad entre los flags: OK (sin flags), WARN o ERROR',

    -- Linaje
    _source_file        STRING          COMMENT 'Archivo de origen (desde bronze)',
    _ingested_at        TIMESTAMP       COMMENT 'Momento de ingesta en bronze',
    _processed_at       TIMESTAMP       COMMENT 'Momento en que se generó la fila en silver'
)
USING DELTA
CLUSTER BY (fecha_operativa)
COMMENT 'Transacciones limpias y enriquecidas - capa silver'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'silver'
);
