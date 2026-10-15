"""Configuración centralizada del pipeline: catálogo, schemas, tablas y rutas.

Cualquier cambio de ambiente (catálogo, nombres de schema) se hace acá y se
propaga a todos los notebooks.
"""

from dataclasses import dataclass

DEFAULT_CATALOG = "workspace"


@dataclass(frozen=True)
class PipelineConfig:
    catalog: str = DEFAULT_CATALOG
    landing_schema: str = "landing"
    landing_volume: str = "raw_files"
    bronze_schema: str = "bronze"
    checkpoints_volume: str = "checkpoints"

    # --- Landing -------------------------------------------------------------
    def landing_path(self, source: str) -> str:
        """Carpeta del volumen de landing donde aterrizan los archivos de una fuente."""
        return f"/Volumes/{self.catalog}/{self.landing_schema}/{self.landing_volume}/{source}"

    # --- Bronze --------------------------------------------------------------
    def bronze_table(self, source: str) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{source}"

    def checkpoint_path(self, source: str) -> str:
        """Checkpoint de Auto Loader: registra qué archivos ya se ingirieron."""
        return (
            f"/Volumes/{self.catalog}/{self.bronze_schema}/{self.checkpoints_volume}"
            f"/{source}/checkpoint"
        )
