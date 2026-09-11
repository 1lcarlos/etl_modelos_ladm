-- Destino: gc_estructuraprediomatriznpn (estructura de la unidad predial que apunta a su matriz)
-- Origen : gc_prediocopropiedad (unidad_predial -> matriz)
WITH datos AS (
    SELECT
        cp.id,
        cp.unidad_predial,
        pm.numero_predial AS npn_matriz,
        pm.area           AS area_terreno_matriz
    FROM {schema}.gc_prediocopropiedad AS cp
    JOIN {schema}.gc_predio AS pm ON pm.id = cp.matriz
    WHERE cp.unidad_predial IS NOT NULL
      AND pm.numero_predial IS NOT NULL
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
