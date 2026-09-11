-- Destino: col_miembros
WITH datos AS (
    SELECT
        m.id,
        m.agrupacion,
        m.interesado_gc_interesado,
        m.participacion
    FROM {schema}.col_miembros AS m
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
