-- Destino: gc_unidadconstruccion
-- Todas las unidades con geometria (tengan o no construccion o caracteristica asociada).
-- tipo_planta viene de la caracteristica (en Django esta alli; en v3 esta en la unidad).
WITH datos AS (
    SELECT
        u.id,
        u.codigo,
        u.etiqueta,
        u.area,
        u.observacion,
        u.geometria,
        u.planta_ubicacion,
        u.altura,
        u.gc_caracteristicasunidadconstruccion AS id_caracteristica,
        u.comienzo_vida_util_version,
        u.fin_vida_util_version,
        cp.text_code AS tipo_planta
    FROM {schema}.gc_unidadconstruccion AS u
    LEFT JOIN {schema}.gc_caracteristicasunidadconstruccion AS c ON c.id = u.gc_caracteristicasunidadconstruccion
    LEFT JOIN {schema}.gc_construccionplantatipo AS cp ON cp.id = c.tipo_planta
    WHERE u.geometria IS NOT NULL
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
