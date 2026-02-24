-- Query para extraer datos de gc_terreno para migrar a cr_terreno
-- Origen: {schema}.gc_terreno
-- Destino: sinic2.cr_terreno
-- Fecha: 2026-02-02

SELECT DISTINCT ON (t.id)
    t.id,
    COALESCE(t.espacio_de_nombres, 'CR_TERRENO') as espacio_de_nombres,
    COALESCE(t.local_id, t.id::varchar) as local_id,
    COALESCE(t.comienzo_vida_util_version, NOW()) as comienzo_vida_util_version,
    t.fin_vida_util_version,
    t.geometria,
    t.etiqueta,
    t.codigo,
    t.area,
    'En_Rasante' as relacion_superficie
FROM {schema}.gc_terreno t
WHERE t.geometria IS NOT NULL
ORDER BY t.id;
