-- Query para extraer relacion predio-unidadconstruccion para migrar a col_uebaunit
-- Origen: {schema}.col_uebaunit
-- Destino: sinic2.col_uebaunit
-- Fecha: 2026-02-02

SELECT DISTINCT ON (ub.id)
    ub.id,
    ub.unidad as predio_id,
    NULL::bigint as terreno_id,
    ub.ue_gc_unidadconstruccion as unidadconstruccion_id
FROM {schema}.col_uebaunit ub
WHERE ub.ue_gc_unidadconstruccion IS NOT NULL
ORDER BY ub.id;
