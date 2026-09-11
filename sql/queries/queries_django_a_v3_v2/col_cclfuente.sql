-- Destino: col_cclfuente (lindero -> fuente espacial)
WITH datos AS (
    SELECT c.id, c.ccl, c.fuente_espacial
    FROM {schema}.col_cclfuente AS c
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
