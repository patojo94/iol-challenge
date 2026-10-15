#!/usr/bin/env bash
# =============================================================================
# Sube el dataset local al volumen de landing en Databricks.
# Requiere Databricks CLI y las variables DATABRICKS_HOST y DATABRICKS_TOKEN
# definidas en .env (ver .env.dev).
# Uso: ./setup/03_upload_data.sh [ruta_csv]
# =============================================================================
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SOURCE_FILE="${1:-$REPO_ROOT/demo-data/data-set-challenge-5-.csv}"
TARGET_DIR="dbfs:/Volumes/workspace/landing/raw_files/transacciones"

if [[ -f "$REPO_ROOT/.env" ]]; then
  set -a; source "$REPO_ROOT/.env"; set +a
fi

: "${DATABRICKS_HOST:?Falta DATABRICKS_HOST en .env}"
: "${DATABRICKS_TOKEN:?Falta DATABRICKS_TOKEN en .env}"
[[ -f "$SOURCE_FILE" ]] || { echo "No existe el archivo: $SOURCE_FILE" >&2; exit 1; }

databricks fs mkdirs "$TARGET_DIR"
databricks fs cp --overwrite "$SOURCE_FILE" "$TARGET_DIR/"
databricks fs ls "$TARGET_DIR"
