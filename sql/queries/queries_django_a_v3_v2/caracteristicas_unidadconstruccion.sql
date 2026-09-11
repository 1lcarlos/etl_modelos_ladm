-- Destino: gc_caracteristicasunidadconstruccion
-- Solo las caracteristicas usadas por unidades de construccion con geometria.
-- tipo_construccion y puntaje no tienen columna en v3 (se reportan como NO_MIGRADO).
WITH usadas AS (
    SELECT DISTINCT u.gc_caracteristicasunidadconstruccion AS id
    FROM {schema}.gc_unidadconstruccion AS u
    WHERE u.geometria IS NOT NULL
      AND u.gc_caracteristicasunidadconstruccion IS NOT NULL
),
datos AS (
    SELECT
        c.id,
        c.identificador,
        c.total_plantas,
        c.anio_construccion,
        c.area,
        c.observacion,
        c.total_habitaciones,
        c.total_banios,
        c.total_locales,
        c.puntaje,
        tu.text_code AS tipo_unidad_construccion,
        us.text_code AS uso,
        ct.text_code AS tipo_construccion
    FROM {schema}.gc_caracteristicasunidadconstruccion AS c
    JOIN usadas ON usadas.id = c.id
    LEFT JOIN {schema}.gc_unidadconstrucciontipo AS tu ON tu.id = c.tipo_unidad_construccion
    LEFT JOIN {schema}.gc_usouconstipo AS us ON us.id = c.uso
    LEFT JOIN {schema}.gc_construcciontipo AS ct ON ct.id = c.tipo_construccion
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
