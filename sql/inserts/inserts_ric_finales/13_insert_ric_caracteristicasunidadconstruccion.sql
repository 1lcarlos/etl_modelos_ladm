-- INSERT para tabla ric_caracteristicasunidadconstruccion
-- Migra caracteristicas de unidad de construccion a la estructura RIC
-- Fecha: 2025-12-18
--
-- Dependencias:
--   - Usa datos de tmp_caracteristicasunidadconstruccion (query caracteristicasunidadconstruccion.sql)

INSERT INTO ric.ric_caracteristicasunidadconstruccion (
    t_id,
    t_ili_tid,
    identificador,
    tipo_construccion,
    tipo_dominio,
    tipo_unidad_construccion,
    total_habitaciones,
    total_banios,
    total_locales,
    total_plantas,
    uso,
    anio_construccion,
    area_construida,
    area_privada_construida,
    observaciones,
    comienzo_vida_util_version,
    fin_vida_util_version,
    espacio_de_nombres,
    local_id
)
SELECT
    
    --nextval('ric.t_ili2db_seq'::regclass),
    c.id::bigint,
    uuid_generate_v4(),

    -- identificador (NOT NULL)
    COALESCE(c.identificador, 'A'),

    -- tipo_construccion (puede ser NULL)
    NULL,

    -- tipo_dominio (puede ser NULL)
    NULL,

    -- tipo_unidad_construccion (NOT NULL): Mapeo a ric_unidadconstrucciontipo
    COALESCE(
        (SELECT t_id FROM ric.ric_unidadconstrucciontipo
         WHERE ilicode ILIKE '%' || c.tipo_unidad_construccion || '%'
         LIMIT 1),
        (SELECT t_id FROM ric.ric_unidadconstrucciontipo
         WHERE ilicode = 'Anexo'
         LIMIT 1)
    ),

    -- total_habitaciones
    c.total_habitaciones::integer,

    -- total_banios
    c.total_banios::integer,

    -- total_locales
    c.total_locales::integer,

    -- total_plantas
    COALESCE(c.total_plantas::integer, 1),

    -- uso: Mapeo a ric_usouconstipo
    (SELECT t_id FROM ric.ric_usouconstipo
     WHERE ilicode ILIKE '%' || c.uso || '%'
     LIMIT 1),

    -- anio_construccion
    COALESCE(c.anio_construccion::integer, null::integer),
   
    -- area_construida (NOT NULL)
    COALESCE(c.area_construida::numeric(15,1), 0),

    -- area_privada_construida
    NULL::integer,

    -- observaciones
    c.observaciones,

    -- comienzo_vida_util_version (NOT NULL)
    NOW(),

    -- fin_vida_util_version
    NULL::timestamp,

    -- espacio_de_nombres (NOT NULL)
    'RIC_CARACTERISTICAS_UC',

    -- local_id (NOT NULL)
    c.id::varchar

FROM tmp_caracteristicasunidadconstruccion c;
