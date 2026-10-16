"""Fixtures compartidos.

Los tests que usan `spark` corren con PySpark local y se saltean si no está
instalado (requiere Java). Los que además usan `delta_spark` necesitan el
paquete `delta-spark` para ejecutar MERGE.
"""

import pytest


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    pyspark = pytest.importorskip("pyspark")
    builder = (
        pyspark.sql.SparkSession.builder.master("local[1]")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
    )
    try:
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
    except ImportError:
        pass
    session = builder.getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def delta_spark(spark):
    pytest.importorskip("delta")
    return spark
