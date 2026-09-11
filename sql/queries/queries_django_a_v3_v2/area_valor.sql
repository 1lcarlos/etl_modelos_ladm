-- Destino: col_areavalor (estructura de terreno y de unidad de construccion)
-- Areas registradas en Django para terrenos y unidades que se migran (con geometria).
WITH datos AS (
    SELECT 'TERRENO' AS tipo_entidad, t.id AS id_entidad, t.area
    FROM {schema}.gc_terreno AS t
    WHERE t.area IS NOT NULL
      AND t.geometria IS NOT NULL
    UNION ALL
    SELECT 'UNIDAD_CONSTRUCCION', u.id, u.area
    FROM {schema}.gc_unidadconstruccion AS u
    WHERE u.area IS NOT NULL
      AND u.geometria IS NOT NULL
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
