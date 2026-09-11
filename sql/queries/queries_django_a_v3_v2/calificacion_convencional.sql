-- Destino: gc_calificacionconvencional
WITH datos AS (
    SELECT
        cc.id,
        cc.total_calificacion,
        cc.gc_caracteristicasunidadconstruccion AS id_caracteristica,
        ct.text_code AS tipo_calificar
    FROM {schema}.cuc_calificacionconvencional AS cc
    LEFT JOIN {schema}.cuc_calificartipo AS ct ON ct.id = cc.tipo_calificar
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
