-- Destino: col_puntofuente (punto -> fuente espacial)
WITH datos AS (
    SELECT
        p.id,
        p.fuente_espacial,
        p.punto_gc_puntolindero,
        p.punto_gc_puntocontrol,
        p.punto_gc_puntolevantamiento
    FROM {schema}.col_puntofuente AS p
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
