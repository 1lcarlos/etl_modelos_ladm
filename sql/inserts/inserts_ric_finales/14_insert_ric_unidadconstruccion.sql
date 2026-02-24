-- INSERT para tabla ric_unidadconstruccion
-- Migra unidades de construccion a la estructura RIC
-- Fecha: 2025-12-18
--
-- Dependencias:
--   - Requiere que existan registros en ric_construccion y ric_caracteristicasunidadconstruccion
--   - Usa datos de tmp_unidadconstruccion (query unidadconstruccion.sql)

INSERT INTO ric.ric_unidadconstruccion (
    t_id,
    t_ili_tid,
    planta_ubicacion,
    area_construida,
    altura,
    geometria,
    ric_caracteristicasunidadconstruccion,
    ric_construccion,
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
    --nextval('ric.t_ili2db_seq'::regclass),
    u.id::bigint,
    uuid_generate_v4(),

    -- planta_ubicacion (NOT NULL)
    COALESCE(u.planta_ubicacion::integer, 1)::integer as planta_ubicacion,

    -- area_construida (NOT NULL)
    COALESCE(ST_Area(u.geometria)::numeric(15,1), 0),

    -- altura
    round(u.altura::numeric(1,0),0), 
    
    -- geometria (NOT NULL): Transformar a MultiPolygonZ SRID 9377
    /* CASE
        WHEN ST_GeometryType(u.geometria) = 'ST_MultiPolygon' THEN
            ST_Force3D(ST_Transform(u.geometria, 9377))
        WHEN ST_GeometryType(u.geometria) = 'ST_Polygon' THEN
            ST_Force3D(ST_Transform(ST_Multi(u.geometria), 9377))
        ELSE
            ST_Force3D(ST_Transform(ST_Multi(ST_MakeValid(u.geometria)), 9377))
    END, */
    /* ST_RemoveRepeatedPoints(ST_Simplify(u.geometria,0.1)),
 */

  ST_Force3D(
        ST_SnapToGrid(
            ST_Transform(
                ST_CollectionExtract(ST_MakeValid(u.geometria),3),
                9377
            ),0.001
        )
    ),

    -- ric_caracteristicasunidadconstruccion (NOT NULL): Buscar por local_id
    (SELECT rc.t_id
     FROM ric.ric_caracteristicasunidadconstruccion rc
     WHERE rc.local_id::integer = u.cr_caracteristicasunidadconstruccion::integer
     LIMIT 1),

    -- ric_construccion (NOT NULL): Buscar construccion relacionada
    -- Nota: En el modelo gc_ la relacion es a traves de caracteristicas
    (SELECT rcon.t_id
     FROM ric.ric_construccion rcon
     WHERE rcon.local_id::integer = u.cr_construccion::integer
     LIMIT 1),

    -- dimension
    (SELECT t_id FROM ric.col_dimensiontipo WHERE ilicode = 'Dim2D' LIMIT 1),

    -- etiqueta
    u.etiqueta,

    -- relacion_superficie
    (SELECT t_id FROM ric.col_relacionsuperficietipo WHERE ilicode = 'En_Rasante' LIMIT 1),

    -- nivel
    NULL,

    -- comienzo_vida_util_version (NOT NULL)
    COALESCE(u.comienzo_vida_util_version::timestamp, NOW()),

    -- fin_vida_util_version
    u.fin_vida_util_version::timestamp,

    -- espacio_de_nombres (NOT NULL)
    COALESCE(u.espacio_de_nombres, 'RIC_UNIDAD_CONSTRUCCION'),

    -- local_id (NOT NULL)
    COALESCE(u.id::varchar, u.local_id::varchar)

FROM tmp_unidadconstruccion u
WHERE u.geometria IS NOT NULL
  AND EXISTS (
      SELECT 1 FROM ric.ric_caracteristicasunidadconstruccion rc
      WHERE rc.local_id::integer = u.cr_caracteristicasunidadconstruccion::integer
  )AND EXISTS (
      SELECT 1 FROM ric.ric_construccion rcon
      WHERE rcon.local_id::integer = u.cr_construccion::integer
  );
