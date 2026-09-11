-- Destino: col_uefuente (terreno o unidad de construccion -> fuente espacial)
WITH datos AS (
    SELECT
        u.id,
        u.fuente_espacial,
        u.ue_gc_terreno AS ue_terreno,
        u.ue_gc_unidadconstruccion AS ue_unidadconstruccion,
        u.ue_gc_construccion AS ue_construccion,
        u.ue_gc_servidumbretransito AS ue_servidumbre
    FROM {schema}.col_uefuente AS u
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
