-- =============================================================================
-- Zona Landing: archivos crudos tal como llegan de las fuentes.
-- No contiene tablas, solo un volumen con una carpeta por fuente.
-- Ver DT-008 en docs/TECHNICAL_DECISIONS.md.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS workspace.landing
COMMENT 'Zona de aterrizaje de archivos crudos de las fuentes, previa a la capa bronze';

CREATE VOLUME IF NOT EXISTS workspace.landing.raw_files
COMMENT 'Archivos fuente tal como llegan, organizados en una carpeta por fuente';
