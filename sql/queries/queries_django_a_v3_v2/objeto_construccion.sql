-- Destino: gc_objetoconstruccion
WITH datos AS (
    SELECT
        o.id,
        o.puntos,
        o.cuc_grupo_calificacion AS id_grupo,
        ot.text_code AS tipo_objeto_construccion
    FROM {schema}.cuc_objetoconstruccion AS o
    LEFT JOIN {schema}.cuc_objetoconstrucciontipo AS ot ON ot.id = o.tipo_objeto_construccion
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
