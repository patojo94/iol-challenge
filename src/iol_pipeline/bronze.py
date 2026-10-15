"""Ingesta de archivos de landing a la capa Bronze con Auto Loader.

Bronze es una copia fiel de la fuente (DT-003): no se filtra ni se corrige
ningún registro. Los valores que no matchean el schema quedan en
`_rescued_data` y las reglas de calidad se aplican en Silver.

PySpark se importa dentro de las funciones que lo necesitan para que la
lógica pura (schema declarativo, evaluación de resultados) se pueda testear
sin cluster ni Spark instalado.
"""

from dataclasses import asdict, dataclass

# Schema de la fuente, en el orden del CSV. Debe coincidir con
# setup/02_bronze.sql (lo verifica tests/test_bronze.py).
# Se declara explícito en lugar de inferirlo: en datos financieros la
# inferencia es riesgosa (ej. `cantidad` llega a 1.500M y desbordaría un INT).
TRANSACCIONES_COLUMNS: list[tuple[str, str]] = [
    ("fecha", "TIMESTAMP"),
    ("tipoTran", "STRING"),
    ("id_cliente", "STRING"),
    ("descripcion_titulo", "STRING"),
    ("moneda", "STRING"),
    ("simbolo_titulo", "STRING"),
    ("cantidad", "DECIMAL(20,6)"),
    ("precio", "DECIMAL(20,6)"),
    ("id_transaccion", "STRING"),
    ("origen", "STRING"),
]

RESCUED_DATA_COLUMN = "_rescued_data"

STATUS_OK = "OK"
STATUS_WARN = "WARN"
STATUS_FAIL = "FAIL"


def to_ddl_schema(columns: list[tuple[str, str]]) -> str:
    """Convierte la lista de columnas al formato DDL que acepta `DataFrameReader.schema()`."""
    return ", ".join(f"{name} {dtype}" for name, dtype in columns)


@dataclass
class IngestionResult:
    """Resultado de una corrida de ingesta, con los controles de integridad."""

    table: str
    files_processed: int
    source_rows: int
    rows_written: int
    rescued_rows: int
    status: str
    message: str

    def to_dict(self) -> dict:
        return asdict(self)


def evaluate_ingestion(
    table: str,
    files_processed: int,
    source_rows: int,
    rows_written: int,
    rescued_rows: int,
) -> IngestionResult:
    """Evalúa la integridad de una corrida de ingesta.

    - FAIL: las filas escritas no coinciden con las filas de los archivos
      procesados (se perdieron o duplicaron registros).
    - WARN: hay registros con valores rescatados; la carga es íntegra pero
      Silver debe tratarlos.
    - OK: todo lo que entró quedó en Bronze. Incluye la corrida sin archivos
      nuevos (0 = 0), que es el caso esperado al re-ejecutar (idempotencia).
    """
    if rows_written != source_rows:
        status = STATUS_FAIL
        message = (
            f"Filas escritas ({rows_written}) distintas de filas en los archivos "
            f"procesados ({source_rows})"
        )
    elif rescued_rows > 0:
        status = STATUS_WARN
        message = f"{rescued_rows} registros con valores en {RESCUED_DATA_COLUMN}"
    elif files_processed == 0:
        status = STATUS_OK
        message = "Sin archivos nuevos para ingerir"
    else:
        status = STATUS_OK
        message = f"{rows_written} filas ingeridas desde {files_processed} archivo(s)"

    return IngestionResult(
        table=table,
        files_processed=files_processed,
        source_rows=source_rows,
        rows_written=rows_written,
        rescued_rows=rescued_rows,
        status=status,
        message=message,
    )


def ingest_csv_to_bronze(
    spark,
    source_path: str,
    target_table: str,
    checkpoint_path: str,
    columns: list[tuple[str, str]],
    ingested_at,
) -> None:
    """Ingesta incremental de CSV a una tabla Delta Bronze con Auto Loader.

    Idempotencia a nivel archivo: el checkpoint registra los archivos ya
    procesados, así que re-ejecutar no duplica datos. `availableNow` procesa
    todo lo pendiente y termina (modo batch, compatible con serverless).

    `ingested_at` es un timestamp fijo por corrida: identifica qué filas
    entraron en esta ejecución para poder validarlas.
    """
    from pyspark.sql import functions as F

    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("rescuedDataColumn", RESCUED_DATA_COLUMN)
        .schema(to_ddl_schema(columns))
        .load(source_path)
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_modification_time").alias("_file_modified_at"),
            F.lit(ingested_at).cast("timestamp").alias("_ingested_at"),
        )
    )

    query = (
        stream.writeStream.option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable(target_table)
    )
    query.awaitTermination()


def validate_ingestion(spark, target_table: str, ingested_at) -> IngestionResult:
    """Compara lo escrito en Bronze en esta corrida contra los archivos de origen."""
    from pyspark.sql import functions as F

    written = spark.table(target_table).where(
        F.col("_ingested_at") == F.lit(ingested_at).cast("timestamp")
    )
    stats = written.agg(
        F.count("*").alias("rows_written"),
        F.count(RESCUED_DATA_COLUMN).alias("rescued_rows"),
        F.collect_set("_source_file").alias("files"),
    ).first()

    files = list(stats["files"])
    source_rows = (
        spark.read.option("header", "true").csv(files).count() if files else 0
    )

    return evaluate_ingestion(
        table=target_table,
        files_processed=len(files),
        source_rows=source_rows,
        rows_written=stats["rows_written"],
        rescued_rows=stats["rescued_rows"],
    )
