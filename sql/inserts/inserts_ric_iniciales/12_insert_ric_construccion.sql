-- INSERT para tabla ric_construccion
-- Migra construcciones desde la tabla temporal a la estructura RIC
-- Fecha: 2025-12-18
--
-- Dependencias:
--   - Usa datos de tmp_gc_construccion (query gc_construccion.sql)

INSERT INTO ric.ric_construccion (
    t_id,
    t_ili_tid,
    identificador,
    tipo_construccion,
    tipo_dominio,
    numero_pisos,
    numero_sotanos,
    numero_mezanines,
    numero_semisotanos,
    anio_construccion,
    avaluo_construccion,
    area_construccion,
    altura,
    observaciones,
    codigo_construccion,
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
    nextval('ric.t_ili2db_seq'::regclass),
    uuid_generate_v4(),

    -- identificador (NOT NULL)
    COALESCE(c.identificador, c.codigo, 'CONS_' || c.id::varchar),

    -- tipo_construccion: Mapeo a ric_construcciontipo
    (SELECT t_id FROM ric.ric_construcciontipo
     WHERE ilicode ILIKE '%' || c.tipo_construccion || '%'
     LIMIT 1),

    -- tipo_dominio: Mapeo a ric_dominioconstrucciontipo
    (SELECT t_id FROM ric.ric_dominioconstrucciontipo
     WHERE ilicode ILIKE '%' || c.tipo_dominio || '%'
     LIMIT 1),

    -- numero_pisos (NOT NULL)
    COALESCE(c.numero_pisos, 1),

    -- numero_sotanos
    c.numero_sotanos,

    -- numero_mezanines
    c.numero_mezanines,

    -- numero_semisotanos
    c.numero_semisotanos,

    -- anio_construccion
    CASE
        WHEN c.anio_construccion IS NULL THEN NULL
        WHEN c.anio_construccion < 1550 THEN 1550
        WHEN c.anio_construccion > 2500 THEN 2500
        ELSE c.anio_construccion
    END,

    -- avaluo_construccion
    0,

    -- area_construccion (NOT NULL)
    COALESCE(c.area_construccion::numeric(15,1), 0),

    -- altura
    c.altura,

    -- observaciones
    c.observaciones,

    -- codigo_construccion
    c.codigo,

    -- geometria (NOT NULL): Transformar a MultiPolygonZ SRID 9377
    CASE
        WHEN ST_GeometryType(c.geometria) = 'ST_MultiPolygon' THEN
            ST_Force3D(ST_Transform(c.geometria, 9377))
        WHEN ST_GeometryType(c.geometria) = 'ST_Polygon' THEN
            ST_Force3D(ST_Transform(ST_Multi(c.geometria), 9377))
        ELSE
            ST_Force3D(ST_Transform(ST_Multi(ST_MakeValid(c.geometria)), 9377))
    END,

    -- dimension: 2D por defecto
    (SELECT t_id FROM ric.col_dimensiontipo WHERE ilicode = 'Dim2D' LIMIT 1),

    -- etiqueta
    c.etiqueta,

    -- relacion_superficie
    (SELECT t_id FROM ric.col_relacionsuperficietipo WHERE ilicode = 'En_Rasante' LIMIT 1),

    -- nivel
    NULL,

    -- comienzo_vida_util_version (NOT NULL)
    COALESCE(c.comienzo_vida_util_version::timestamp, NOW()),

    -- fin_vida_util_version
    c.fin_vida_util_version::timestamp,

    -- espacio_de_nombres (NOT NULL)
    COALESCE(c.espacio_de_nombres, 'RIC_CONSTRUCCION'),

    -- local_id (NOT NULL)
    COALESCE(c.local_id, c.id::varchar)

FROM tmp_gc_construccion c
WHERE c.geometria IS NOT NULL;
