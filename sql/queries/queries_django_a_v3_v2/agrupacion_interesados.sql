-- Destino: gc_agrupacioninteresados
WITH datos AS (
    SELECT
        ai.id,
        ai.nombre,
        ai.comienzo_vida_util_version,
        ai.fin_vida_util_version,
        git.text_code AS tipo
    FROM {schema}.gc_agrupacioninteresados AS ai
    LEFT JOIN {schema}.col_grupointeresadotipo AS git ON git.id = ai.tipo
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
