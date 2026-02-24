-- Query para extraer datos de col_rrrfuente para migrar
-- Origen: {schema}.col_rrrfuente
-- Destino: sinic2.col_rrrfuente
-- Fecha: 2026-02-02

SELECT DISTINCT ON (rf.id)
    rf.id,
    rf.fuente_administrativa as fuente_administrativa_id,
    rf.rrr_gc_derecho as derecho_id,
    rf.rrr_gc_restriccion as restriccion_id
FROM {schema}.col_rrrfuente rf
ORDER BY rf.id;
