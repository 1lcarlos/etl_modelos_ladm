-- Destino: col_rrrfuente (fuente administrativa -> derecho)
WITH datos AS (
    SELECT
        r.id,
        r.fuente_administrativa,
        r.rrr_gc_derecho,
        r.rrr_gc_restriccion
    FROM {schema}.col_rrrfuente AS r
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
