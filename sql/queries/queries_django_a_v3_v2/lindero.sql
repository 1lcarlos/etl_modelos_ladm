-- Destino: gc_lindero
WITH datos AS (
    SELECT
        l.id,
        l.localizacion_textual,
        l.geometria,
        l.longitud,
        l.comienzo_vida_util_version,
        l.fin_vida_util_version
    FROM {schema}.gc_lindero AS l
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
