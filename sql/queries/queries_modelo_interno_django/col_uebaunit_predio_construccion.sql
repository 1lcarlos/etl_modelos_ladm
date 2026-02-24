-- Query para extraer relacion predio-construccion de col_uebaunit
-- Tabla temporal: tmp_col_uebaunit_predio_construccion
-- Fecha: 2025-12-19

SELECT
    cu.id,
    c.id AS ue_gc_construccion,
    --c.local_id AS local_id_construccion,
    p.id AS unidad
    --,p.local_id AS local_id_predio
FROM {schema}.col_uebaunit cu
JOIN {schema}.gc_construccion c ON cu.ue_gc_construccion = c.id
JOIN {schema}.gc_predio p ON cu.unidad = p.id
WHERE cu.ue_gc_construccion IS NOT NULL;
