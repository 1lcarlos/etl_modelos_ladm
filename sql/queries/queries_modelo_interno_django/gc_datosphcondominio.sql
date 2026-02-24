-- Query para extraer datos de gc_datosphcondominio
-- Tabla temporal: tmp_datosphcondominio
-- Fecha: 2025-12-19

SELECT
    dc.id,
    dc.area_total_terreno,
    dc.area_total_terreno_privada,
    dc.area_total_terreno_comun,
    dc.area_total_construida,
    dc.area_total_construida_privada,
    dc.area_total_construida_comun,
    dc.numero_torres,
    dc.total_unidades_privadas,
    dc.gc_predio,
    -- Referencia para mapeo
    p.numero_predial
FROM {schema}.gc_datosphcondominio dc
LEFT JOIN {schema}.gc_predio p ON dc.gc_predio = p.id
WHERE dc.gc_predio IS NOT NULL;
