-- Query para extraer datos de col_miembros para migrar
-- Origen: {schema}.col_miembros
-- Destino: sinic2.col_miembros
-- Fecha: 2026-02-02

SELECT DISTINCT ON (m.id)
    m.id,
    m.interesado_gc_interesado as interesado_id,
    --m.interesado_gc_agrupacioninteresados as agrupacion_interesado_id,
    m.agrupacion as agrupacion_id,
    COALESCE(m.participacion, 1.0) as participacion
FROM {schema}.col_miembros m
ORDER BY m.id;
