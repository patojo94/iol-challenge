-- =============================================================================
-- Capa Gold: modelo dimensional en estrella (DT-015).
--
--     dim_fecha ─┐
--     dim_canal ─┼── fact_operaciones ──┬─ dim_instrumento
--                │                      └─ dim_cliente (SCD Tipo 2)
--
-- Claves subrogadas deterministas (xxhash64 de la clave natural).
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS workspace.gold
COMMENT 'Modelo dimensional para consumo analítico';

CREATE TABLE IF NOT EXISTS workspace.gold.dim_fecha (
    fecha_key           INT             COMMENT 'Clave AAAAMMDD',
    fecha               DATE            COMMENT 'Fecha calendario',
    anio                INT             COMMENT 'Año',
    mes                 INT             COMMENT 'Mes (1-12)',
    nombre_mes          STRING          COMMENT 'Nombre del mes',
    trimestre           INT             COMMENT 'Trimestre (1-4)',
    dia                 INT             COMMENT 'Día del mes',
    dia_semana          INT             COMMENT 'Día de la semana ISO (1 = lunes, 7 = domingo)',
    nombre_dia          STRING          COMMENT 'Nombre del día',
    es_fin_de_semana    BOOLEAN         COMMENT 'Sábado o domingo',
    es_feriado          BOOLEAN         COMMENT 'Feriado nacional con mercado cerrado',
    nombre_feriado      STRING          COMMENT 'Nombre del feriado (NULL si no es feriado)',
    es_dia_habil        BOOLEAN         COMMENT 'Día hábil de mercado'
)
USING DELTA
COMMENT 'Dimensión calendario';

CREATE TABLE IF NOT EXISTS workspace.gold.dim_canal (
    canal_key           BIGINT          COMMENT 'Clave subrogada: xxhash64(canal)',
    canal               STRING          COMMENT 'Canal de origen de la operación',
    tipo_canal          STRING          COMMENT 'APP, WEB, API u OTRO'
)
USING DELTA
COMMENT 'Dimensión de canales de operación';

CREATE TABLE IF NOT EXISTS workspace.gold.dim_instrumento (
    instrumento_key         BIGINT      COMMENT 'Clave subrogada: xxhash64(simbolo)',
    simbolo                 STRING      COMMENT 'Ticker operado',
    simbolo_base            STRING      COMMENT 'Ticker sin sufijo de liquidación',
    tipo_instrumento        STRING      COMMENT 'CEDEAR, BONO, LETRA, ON, OPCION, WARRANT o ACCION',
    tipo_liquidacion        STRING      COMMENT 'ARS, MEP o CABLE',
    moneda                  STRING      COMMENT 'Moneda en que opera el símbolo',
    descripcion             STRING      COMMENT 'Descripción canónica (la más frecuente)',
    descripciones_distintas INT         COMMENT 'Cantidad de descripciones distintas observadas',
    fecha_primera_operacion DATE        COMMENT 'Primera fecha operativa con operaciones',
    fecha_ultima_operacion  DATE        COMMENT 'Última fecha operativa con operaciones'
)
USING DELTA
COMMENT 'Dimensión de instrumentos (un registro por símbolo)';

CREATE TABLE IF NOT EXISTS workspace.gold.dim_cliente (
    cliente_sk              BIGINT      COMMENT 'Clave subrogada de la versión: xxhash64(id_cliente, valid_from)',
    id_cliente              STRING      COMMENT 'Clave natural del cliente',
    segmento_actividad      STRING      COMMENT 'OCASIONAL, RECURRENTE, FRECUENTE o INTENSIVO (operaciones acumuladas) - SCD2',
    canal_principal         STRING      COMMENT 'Canal con más operaciones acumuladas - SCD2',
    fecha_primera_operacion DATE        COMMENT 'Primera fecha operativa del cliente',
    valid_from              DATE        COMMENT 'Inicio de vigencia de la versión',
    valid_to                DATE        COMMENT 'Fin de vigencia (9999-12-31 si está vigente)',
    is_current              BOOLEAN     COMMENT 'Versión vigente',
    _attr_hash              STRING      COMMENT 'Hash de los atributos versionados, para detectar cambios',
    _loaded_at              TIMESTAMP   COMMENT 'Momento de la carga'
)
USING DELTA
CLUSTER BY (id_cliente)
COMMENT 'Dimensión de clientes con historia de atributos (SCD Tipo 2)';

CREATE TABLE IF NOT EXISTS workspace.gold.fact_operaciones (
    id_transaccion      STRING          COMMENT 'Dimensión degenerada: identificador de la transacción',
    fecha_key           INT             COMMENT 'FK a dim_fecha (fecha operativa)',
    cliente_sk          BIGINT          COMMENT 'FK a la versión de dim_cliente vigente en la fecha operativa',
    instrumento_key     BIGINT          COMMENT 'FK a dim_instrumento',
    canal_key           BIGINT          COMMENT 'FK a dim_canal',
    fecha_hora_local    TIMESTAMP       COMMENT 'Fecha y hora de la operación en Argentina',
    tipo_operacion      STRING          COMMENT 'COMPRA o VENTA',
    moneda              STRING          COMMENT 'Moneda de la operación',
    cantidad            DECIMAL(20, 6)  COMMENT 'Cantidad de títulos',
    precio              DECIMAL(20, 6)  COMMENT 'Precio operado',
    monto               DECIMAL(24, 6)  COMMENT 'cantidad x precio',
    precio_mercado      DECIMAL(20, 6)  COMMENT 'Cierre de mercado asignado (Task 04), NULL si SIN_COTIZACION',
    fecha_precio_mercado DATE           COMMENT 'Fecha del cierre usado',
    fuente_precio       STRING          COMMENT 'Fuente del cierre (data912, yahoo)',
    dias_habiles_desfase INT            COMMENT 'Días hábiles entre fecha_operativa y fecha_precio_mercado',
    tipo_precio         STRING          COMMENT 'EXACTO, IMPUTADO (hasta 5 días hábiles antes) o SIN_COTIZACION (DT-020)',
    desvio_vs_mercado_pct DOUBLE        COMMENT '(precio - precio_mercado) / precio_mercado x 100',
    en_horario_mercado  BOOLEAN         COMMENT 'Operación en día hábil y horario de BYMA',
    dq_status           STRING          COMMENT 'OK, WARN o ERROR (desde silver)',
    dq_flags            ARRAY<STRING>   COMMENT 'Flags de calidad (desde silver)',
    _processed_at       TIMESTAMP       COMMENT 'Momento en que se generó la fila'
)
USING DELTA
CLUSTER BY (fecha_key)
COMMENT 'Hechos de operaciones bursátiles (grano: una transacción)';

CREATE TABLE IF NOT EXISTS workspace.gold.cobertura_cotizaciones (
    simbolo                 STRING      COMMENT 'Símbolo',
    tipo_instrumento        STRING      COMMENT 'Tipo de instrumento',
    tipo_liquidacion        STRING      COMMENT 'ARS, MEP o CABLE',
    operaciones             BIGINT      COMMENT 'Operaciones del símbolo',
    ops_precio_exacto       BIGINT      COMMENT 'Operaciones con cierre del mismo día',
    ops_precio_imputado     BIGINT      COMMENT 'Operaciones con cierre de hasta 5 días hábiles antes',
    ops_sin_cotizacion      BIGINT      COMMENT 'Operaciones sin precio de mercado',
    pct_con_precio          DOUBLE      COMMENT '% de operaciones con precio (exacto o imputado)',
    fuente_principal        STRING      COMMENT 'Fuente más usada para el símbolo',
    motivo_sin_cotizacion   STRING      COMMENT 'Por qué faltan precios (errores de API, sin fuente, fuera de ventana)',
    _processed_at           TIMESTAMP   COMMENT 'Momento en que se generó la fila'
)
USING DELTA
COMMENT 'Cobertura de cotizaciones de mercado por símbolo (Task 04)';
