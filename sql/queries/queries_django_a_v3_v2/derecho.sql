-- Destino: gc_derechocatastral (su dominio Dominio/Posesion/Ocupacion coincide con Django;
--          gc_derecho de v3 es el derecho registral y no tiene Posesion ni Ocupacion)
WITH datos AS (
    SELECT
        d.id,
        d.baunit,
        d.interesado_gc_interesado,
        d.interesado_gc_agrupacioninteresados,
        d.descripcion,
        d.fraccion_derecho,
        d.fecha_inicio_tenencia,
        d.comienzo_vida_util_version,
        d.fin_vida_util_version,
        dt.text_code AS tipo
    FROM {schema}.gc_derecho AS d
    LEFT JOIN {schema}.gc_derechotipo AS dt ON dt.id = d.tipo
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
