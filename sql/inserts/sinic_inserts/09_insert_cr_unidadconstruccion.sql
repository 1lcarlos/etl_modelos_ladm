-- INSERT para tabla cr_unidadconstruccion
-- Migra unidades de construccion desde la tabla temporal a SINIC
-- Fecha: 2026-02-02
--
-- Dependencias:
--   - Requiere que cr_caracteristicasunidadconstruccion ya este migrado
--   - Usa datos de tmp_sinic_unidadconstruccion (query cr_unidadconstruccion.sql)

INSERT INTO {schema}.cr_unidadconstruccion (
    t_id,
    --t_basket,
    t_ili_tid,
    tipo_planta,
    planta_ubicacion,
    altura,
    geometria,
    cr_caracteristicasunidadconstruccion,
    etiqueta,
    relacion_superficie,
    comienzo_vida_util_version,
    fin_vida_util_version,
    espacio_de_nombres,
    local_id
)
SELECT
    uc.id::bigint,
    --(SELECT t_id FROM {schema}.t_ili2db_basket LIMIT 1),
    uuid_generate_v4(),

    -- tipo_planta: Mapeo a cr_construccionplantatipo (NOT NULL)
    COALESCE(
        (SELECT t_id FROM {schema}.cr_construccionplantatipo WHERE ilicode = uc.tipo_planta LIMIT 1),
        (SELECT t_id FROM {schema}.cr_construccionplantatipo WHERE ilicode = 'Piso' LIMIT 1)
    ),

    -- planta_ubicacion (NOT NULL)
    COALESCE(uc.planta_ubicacion::numeric, 1),

    -- altura
    uc.altura::numeric,

    -- geometria
    uc.geometria,

    -- cr_caracteristicasunidadconstruccion: referencia a las caracteristicas (NOT NULL)
    COALESCE(
        CASE
            WHEN uc.caracteristicas_id IS NOT NULL
                 AND EXISTS (SELECT 1 FROM {schema}.cr_caracteristicasunidadconstruccion c WHERE c.t_id = uc.caracteristicas_id::numeric)
            THEN uc.caracteristicas_id
            ELSE NULL
        END,
        (SELECT t_id FROM {schema}.cr_caracteristicasunidadconstruccion LIMIT 1)
    ),

    -- etiqueta
    uc.etiqueta,

    -- relacion_superficie: Mapeo a col_relacionsuperficietipo
    COALESCE(
        (SELECT t_id FROM {schema}.col_relacionsuperficietipo WHERE ilicode = uc.relacion_superficie LIMIT 1),
        (SELECT t_id FROM {schema}.col_relacionsuperficietipo WHERE ilicode = 'En_Rasante' LIMIT 1)
    ),

    -- comienzo_vida_util_version (NOT NULL)
    COALESCE(uc.comienzo_vida_util_version::timestamp, NOW()),

    -- fin_vida_util_version (puede ser NULL)
    uc.fin_vida_util_version::timestamp,

    -- espacio_de_nombres (NOT NULL)
    COALESCE(uc.espacio_de_nombres, 'CR_UNIDADCONSTRUCCION'),

    -- local_id (NOT NULL)
    COALESCE(uc.id::varchar, uc.local_id)

FROM tmp_cr_unidadconstruccion uc;
