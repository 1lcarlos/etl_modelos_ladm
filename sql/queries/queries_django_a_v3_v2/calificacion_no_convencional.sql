-- Destino: gc_calificacionnoconvencional
WITH datos AS (
    SELECT
        cn.id,
        cn.gc_caracteristicasunidadconstruccion AS id_caracteristica,
        an.text_code AS tipo_anexo
    FROM {schema}.cuc_calificacionnoconvencional AS cn
    LEFT JOIN {schema}.cuc_anexotipo AS an ON an.id = cn.tipo_anexo
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
