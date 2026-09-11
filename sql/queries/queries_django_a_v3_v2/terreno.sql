-- Destino: gc_terreno (el area va a col_areavalor, ver area_valor.sql)
WITH datos AS (
    SELECT
        t.id,
        t.codigo,
        t.etiqueta,
        t.observacion,
        t.geometria,
        t.comienzo_vida_util_version,
        t.fin_vida_util_version
    FROM {schema}.gc_terreno AS t
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
