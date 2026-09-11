-- Extrae áreas alfanuméricas de terrenos y unidades de construcción
-- Una fila por entidad con área registrada

-- Áreas de terrenos
SELECT
    'Area_Alfanumerica'  AS tipo_area,
    t.area,
    t.local_id           AS terreno_local_id,
    NULL::varchar        AS uc_local_id
FROM {schema}.gc_terreno t
WHERE t.area IS NOT NULL

UNION ALL

-- Áreas de unidades de construcción
SELECT
    'Area_Alfanumerica'  AS tipo_area,
    uc.area,
    NULL::varchar        AS terreno_local_id,
    uc.local_id          AS uc_local_id
FROM {schema}.gc_unidadconstruccion uc
WHERE uc.area IS NOT NULL;
