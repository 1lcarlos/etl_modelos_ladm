-- Destino: cc_sectorurbano (basket LimitesOperativosCatastro)
WITH datos AS (
    SELECT c.id, c.codigo, c.geometria
    FROM {schema}.cc_sectorurbano AS c
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
