-- Destino: col_menosccl
-- Misma regla que col_masccl.
WITH datos AS (
    SELECT
        m.id,
        m.ccl_menos AS ccl,
        m.ue_menos_gc_terreno AS ue_terreno,
        m.ue_menos_gc_unidadconstruccion AS ue_unidadconstruccion,
        m.ue_menos_gc_construccion AS ue_construccion,
        m.ue_menos_gc_servidumbretransito AS ue_servidumbre
    FROM {schema}.col_menosccl AS m
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
