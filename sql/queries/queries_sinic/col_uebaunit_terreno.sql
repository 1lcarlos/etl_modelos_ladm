-- Query para extraer relacion predio-terreno para migrar a col_uebaunit
-- Origen: {schema}.col_uebaunit
-- Destino: sinic2.col_uebaunit
-- Fecha: 2026-02-02

SELECT DISTINCT ON (ub.id)
    ub.id,
    ub.unidad as predio_id,
    ub.ue_gc_terreno as terreno_id,
    NULL::bigint as unidadconstruccion_id
FROM {schema}.col_uebaunit ub
WHERE ub.ue_gc_terreno IS NOT NULL
ORDER BY ub.id;
