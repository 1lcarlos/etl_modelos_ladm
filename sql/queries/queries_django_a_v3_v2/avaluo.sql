-- Destino: gc_estructuraavaluo (historico completo de avaluos del predio)
-- El orden (t_seq) se calcula en el insert, despues de excluir los avaluos marcados BORRAR.
WITH datos AS (
    SELECT
        e.id,
        e.gc_predio_avaluo,
        e.avaluo_catastral,
        e.vigencia,
        e.por_decreto,
        e.descripcion,
        (count(*) OVER (PARTITION BY e.gc_predio_avaluo, e.vigencia) > 1) AS vigencia_duplicada
    FROM {schema}.extavaluo AS e
    WHERE e.gc_predio_avaluo IS NOT NULL
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
