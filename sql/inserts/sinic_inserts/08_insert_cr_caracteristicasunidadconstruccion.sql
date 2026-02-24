-- INSERT para tabla cr_caracteristicasunidadconstruccion
-- Migra caracteristicas de unidades de construccion desde la tabla temporal a SINIC
-- Fecha: 2026-02-02
--
-- Dependencias:
--   - Usa datos de tmp_sinic_caracteristicasunidadconstruccion

INSERT INTO {schema}.cr_caracteristicasunidadconstruccion (
    t_id,
    --t_basket,
    t_ili_tid,
    identificador,
    tipo_unidad_construccion,
    --tipo_dominio,
    uso,
    --total_habitaciones,
    --total_banios,
    --total_locales,
    total_plantas,
    --total_sotanos,
    --total_mezanine,
    anio_construccion,
    --puntuacion,
    --area_privada_construida,
    area_construida, 
    estado_conservacion
    --,comienzo_vida_util_version,
    --fin_vida_util_version,
    --espacio_de_nombres,
    --local_id
)
SELECT
    cuc.id::bigint,
    --(SELECT t_id FROM {schema}.t_ili2db_basket LIMIT 1),
    uuid_generate_v4(),

    -- identificador
    cuc.identificador,

    -- tipo_unidad_construccion: Mapeo a cr_unidadconstrucciontipo
    COALESCE(
        (SELECT t_id FROM {schema}.cr_unidadconstrucciontipo WHERE ilicode = cuc.tipo_unidad_construccion LIMIT 1),
        (SELECT t_id FROM {schema}.cr_unidadconstrucciontipo WHERE ilicode = 'Residencial' LIMIT 1)
    ),

    -- tipo_dominio (puede ser NULL en destino - ajustar segun modelo)
    --NULL,

    -- uso: Mapeo a cr_usouconstipo
    COALESCE(
        (SELECT t_id FROM {schema}.cr_usouconstipo WHERE ilicode = cuc.uso LIMIT 1),
        (SELECT t_id FROM {schema}.cr_usouconstipo WHERE ilicode = 'Residencial.Vivienda_Hasta_3_Pisos' LIMIT 1)
    ),

    -- total_habitaciones
    --COALESCE(cuc.total_habitaciones, 0),

    -- total_banios
    --COALESCE(cuc.total_banios, 0),

    -- total_locales
    --COALESCE(cuc.total_locales, 0),

    -- total_pisos
    COALESCE(cuc.total_pisos, 1),

    -- total_sotanos
    --COALESCE(cuc.total_sotanos, 0),

    -- total_mezanine
    --COALESCE(cuc.total_mezanine, 0),

    -- anio_construccion
    COALESCE(cuc.anio_construccion::integer,1900),

    -- puntuacion
    --cuc.puntuacion,

    -- area_privada_construida
    --COALESCE(cuc.area_privada_construida, 0),

    -- area_construida
    COALESCE(cuc.area_construida::numeric, 0)

    -- estado_conservacion
    ,'Regular'

    --, comienzo_vida_util_version (NOT NULL)
    --COALESCE(cuc.comienzo_vida_util_version::timestamp, NOW()),

    -- fin_vida_util_version (puede ser NULL)
    --cuc.fin_vida_util_version::timestamp,

    -- espacio_de_nombres (NOT NULL)
    --COALESCE(cuc.espacio_de_nombres, 'CR_CARACTERISTICAS'),

    -- local_id (NOT NULL)
    --COALESCE(cuc.local_id, cuc.id::varchar)

FROM tmp_cr_caracteristicasunidadconstruccion cuc;
