-- INSERT para tabla ric_terreno
-- Migra terrenos desde la tabla temporal a la estructura RIC
-- Fecha: 2025-12-18
--
-- Dependencias:
--   - Usa datos de tmp_terreno (query terreno.sql)

INSERT INTO ric.ric_terreno (
    t_id,
    t_ili_tid,
    area_terreno,
    area_digital_gestor,
    geometria,
    dimension,
    etiqueta,
    relacion_superficie,
    nivel,
    comienzo_vida_util_version,
    fin_vida_util_version,
    espacio_de_nombres,
    local_id
)
SELECT
   -- nextval('ric.t_ili2db_seq'::regclass),
    t.id::bigint,
    uuid_generate_v4(),

    -- area_terreno: Calcular desde geometria (NOT NULL)
    COALESCE(
        ST_Area(t.geometria)::numeric(15,1),
        1
    ),

    -- area_digital_gestor: Igual al area calculada
    ST_Area(t.geometria)::numeric(15,1),

    -- geometria: Transformar a MultiPolygonZ SRID 9377 (NOT NULL)
   /*  CASE
        WHEN ST_GeometryType(t.geometria) = 'ST_MultiPolygon' THEN
            ST_Force3D(ST_Transform(t.geometria, 9377))
        WHEN ST_GeometryType(t.geometria) = 'ST_Polygon' THEN
            ST_Force3D(ST_Transform(ST_Multi(t.geometria), 9377))
        ELSE
            ST_Force3D(ST_Transform(ST_Multi(ST_MakeValid(t.geometria)), 9377))
    END, */
   /*  ST_RemoveRepeatedPoints(ST_Simplify(t.geometria,0.1)), */
   ST_Force3D(
        ST_SnapToGrid(
            ST_Transform(
                ST_CollectionExtract(ST_MakeValid(t.geometria),3),
                9377
            ),0.001
        )
    ),


    -- dimension: Mapeo a col_dimensiontipo (2D por defecto)
    (SELECT t_id FROM ric.col_dimensiontipo WHERE ilicode = 'Dim2D' LIMIT 1),

    -- etiqueta
    t.etiqueta,

    -- relacion_superficie: Mapeo a col_relacionsuperficietipo
    (SELECT t_id FROM ric.col_relacionsuperficietipo WHERE ilicode = 'En_Rasante' LIMIT 1),

    -- nivel (puede ser NULL)
    NULL,

    -- comienzo_vida_util_version (NOT NULL)
    COALESCE(t.comienzo_vida_util_version::timestamp, NOW()),

    -- fin_vida_util_version
    t.fin_vida_util_version::timestamp,

    -- espacio_de_nombres (NOT NULL)
    COALESCE(t.espacio_de_nombres, 'RIC_TERRENO'),

    -- local_id (NOT NULL)
    COALESCE(t.id::varchar, t.codigo)

FROM tmp_terreno t
WHERE t.geometria IS NOT NULL;
