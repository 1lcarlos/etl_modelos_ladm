-- Destino: gc_grupocalificacion
WITH datos AS (
    SELECT
        g.id,
        g.subtotal,
        g.cuc_calificacion_convencional AS id_calificacion,
        cc.text_code AS clase_calificacion,
        ec.text_code AS conservacion
    FROM {schema}.cuc_grupocalificacion AS g
    LEFT JOIN {schema}.cuc_clasecalificaciontipo AS cc ON cc.id = g.clase_calificacion
    LEFT JOIN {schema}.cuc_estadoconservaciontipo AS ec ON ec.id = g.conservacion
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
