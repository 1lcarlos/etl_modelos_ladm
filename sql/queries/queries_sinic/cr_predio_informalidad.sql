-- Query para extraer datos de gc_predio_informalidad para migrar a cr_predio_informalidad
-- Origen: {schema}.gc_predio_informalidad
-- Destino: sinic2.cr_predio_informalidad
-- Fecha: 2026-02-02

SELECT DISTINCT ON (pi.id)
    pi.id,
    pi.predio_formal as predio_formal_id,
    pi.predio_informal as predio_informal_id
FROM {schema}.gc_predio_informalidad pi
ORDER BY pi.id;
