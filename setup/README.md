# Setup de Databricks

Scripts para crear la estructura del workspace. Se ejecutan **una vez**, en orden, antes de correr los notebooks del pipeline. Todos son idempotentes (`IF NOT EXISTS`), así que se pueden volver a ejecutar sin efectos.

| Orden | Script | Qué hace | Dónde se ejecuta |
|---|---|---|---|
| 01 | `01_landing.sql` | Crea el schema `landing` y el volumen `raw_files` | SQL Editor o notebook |
| 02 | `02_bronze.sql` | Crea el schema `bronze`, el volumen `checkpoints` y la tabla `transacciones` | SQL Editor o notebook |
| 03 | `03_upload_data.sh` | Sube el CSV a `/Volumes/workspace/landing/raw_files/transacciones/` | Terminal local (Databricks CLI) |
| 04 | `04_silver.sql` | Crea el schema `silver` y las tablas `ref_feriados` y `transacciones` | SQL Editor o notebook |
| 05 | `05_gold.sql` | Crea el schema `gold`, las dimensiones y `fact_operaciones` | SQL Editor o notebook |

**Alternativa manual al paso 03:** Catalog → `workspace` → `landing` → `raw_files` → crear la carpeta `transacciones` → *Upload to this volume*.
