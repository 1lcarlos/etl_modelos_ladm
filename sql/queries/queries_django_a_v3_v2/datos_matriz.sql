-- Destino: gc_datosmatriz
-- Origen : gc_datosphcondominio
WITH datos AS (
    SELECT
        dc.id,
        dc.gc_predio,
        dc.area_total_terreno,
        dc.area_total_terreno_privada,
        dc.area_total_terreno_comun,
        dc.area_total_construida,
        dc.area_total_construida_privada,
        dc.area_total_construida_comun,
        dc.numero_torres,
        dc.total_unidades_privadas
    FROM {schema}.gc_datosphcondominio AS dc
    WHERE dc.gc_predio IS NOT NULL
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
