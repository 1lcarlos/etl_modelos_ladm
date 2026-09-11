-- Destino: gc_estructuraavaluocaracterizacion
-- Origen : extavaluo asociado a unidad de construccion -> su caracteristica
WITH datos AS (
    SELECT
        e.id,
        e.avaluo_catastral,
        e.vigencia,
        u.gc_caracteristicasunidadconstruccion AS id_caracteristica,
        (row_number() OVER (PARTITION BY u.gc_caracteristicasunidadconstruccion
                            ORDER BY e.vigencia DESC NULLS LAST, e.id) - 1) AS orden
    FROM {schema}.extavaluo AS e
    JOIN {schema}.gc_unidadconstruccion AS u ON u.id = e.gc_unidadconstruccion_avaluo
    WHERE u.gc_caracteristicasunidadconstruccion IS NOT NULL
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
