-- Destino: col_baunitfuente (predio -> fuente espacial)
WITH datos AS (
    SELECT b.id, b.unidad, b.fuente
    FROM {schema}.col_baunitfuente AS b
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
