-- Destino: col_masccl
-- En v3 solo existen terreno y unidad de construccion como UE; construccion y servidumbre
-- se reportan como NO_MIGRADO en el insert.
WITH datos AS (
    SELECT
        m.id,
        m.ccl_mas AS ccl,
        m.ue_mas_gc_terreno AS ue_terreno,
        m.ue_mas_gc_unidadconstruccion AS ue_unidadconstruccion,
        m.ue_mas_gc_construccion AS ue_construccion,
        m.ue_mas_gc_servidumbretransito AS ue_servidumbre
    FROM {schema}.col_masccl AS m
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
