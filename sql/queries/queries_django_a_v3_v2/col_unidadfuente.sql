-- Destino: col_unidadfuente (fuente administrativa -> predio)
WITH datos AS (
    SELECT
        u.id,
        u.unidad,
        u.fuente_administrativa
    FROM {schema}.col_unidadfuente AS u
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
