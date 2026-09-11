-- Destino: col_puntoccl (punto -> lindero)
-- punto_gc_puntolevantamiento no existe en v3: el insert lo reporta como NO_MIGRADO.
WITH datos AS (
    SELECT
        p.id,
        p.ccl,
        p.punto_gc_puntolindero,
        p.punto_gc_puntocontrol,
        p.punto_gc_puntolevantamiento
    FROM {schema}.col_puntoccl AS p
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
