-- Destino: gc_predio_copropiedad
-- area_terreno_matriz se usa para calcular area_catastral_terreno_coeficiente = coeficiente * area.
WITH datos AS (
    SELECT
        cp.id,
        cp.coeficiente,
        cp.matriz,
        cp.unidad_predial,
        pm.area AS area_terreno_matriz
    FROM {schema}.gc_prediocopropiedad AS cp
    JOIN {schema}.gc_predio AS pm ON pm.id = cp.matriz
    WHERE cp.unidad_predial IS NOT NULL
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
