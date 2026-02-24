-- INSERT para tabla gc_caracteristicasunidadconstruccion (Modelo Interno Django)
-- Migra caracteristicas de unidad de construccion desde modelo CCA
-- Fecha: 2026-02-06
--
-- Origen: tmp_cca_caracteristicasunidadconstruccion (query cca_caracteristicasunidadconstruccion.sql)
-- Destino: gc_caracteristicasunidadconstruccion (modelo interno Django)
--
-- Diferencias clave con modelo INTERLIS:
--   - PK es 'id' (se usa cca_caracteristicas_id)
--   - No existe t_ili_tid
--   - Dominios usan text_code (no ilicode) y FK apunta a 'id' (no t_id)
--   - Campo 'puntaje' en Django viene de calificacion_convencional.total_calificacion en CCA
--   - tipo_anexo y tipo_tipologia no son columnas directas en Django
--     (se manejan en cuc_calificacionnoconvencional y cuc_tipologiaconstruccion)
--
-- Dependencias:
--   - No depende directamente de gc_predio (tabla independiente)
--   - Las tablas de dominio deben estar pobladas
--
-- IMPORTANTE: Este insert puede ejecutarse en paralelo con gc_construccion

INSERT INTO {schema}.gc_caracteristicasunidadconstruccion (
    id,
    espacio_de_nombres,
    local_id,
    comienzo_vida_util_version,
    fin_vida_util_version,
    identificador,
    total_habitaciones,
    total_banios,
    total_locales,
    total_plantas,
    area,
    anio_construccion,
    area_privada_construida,
    puntaje,
    codigo,
    observacion,
    tipo_construccion,
    tipo_dominio,
    tipo_planta,
    tipo_unidad_construccion,
    uso
)
SELECT
    -- id: Usar cca_caracteristicas_id como id en Django
    cu.cca_caracteristicas_id,

    -- espacio_de_nombres
    'GC_CARACT_UCONS_CCA',

    -- local_id
    cu.cca_caracteristicas_id::varchar,

    -- comienzo_vida_util_version
    NOW(),

    -- fin_vida_util_version
    NULL,

    -- identificador (NOT NULL)
    cu.identificador,

    -- total_habitaciones
    cu.total_habitaciones,

    -- total_banios
    cu.total_banios,

    -- total_locales
    cu.total_locales,

    -- total_plantas
    cu.total_plantas,

    -- area
    COALESCE(cu.area_construida::numeric(16,2), 0),

    -- anio_construccion
    cu.anio_construccion,

    -- area_privada_construida
    cu.area_privada_construida::numeric(16,2),

    -- puntaje: Viene de calificacion_convencional.total_calificacion
    cu.puntaje::numeric,

    -- codigo
    NULL,

    -- observacion
    cu.observaciones,

    -- tipo_construccion: Mapeo ilicode (CCA) -> text_code (Django) -> id
    COALESCE(
        (SELECT id FROM {schema}.gc_construcciontipo
         WHERE text_code = cu.tipo_construccion LIMIT 1),
        (SELECT id FROM {schema}.gc_construcciontipo
         WHERE text_code ILIKE '%' || cu.tipo_construccion || '%' LIMIT 1),
        NULL
    ),

    -- tipo_dominio: Mapeo ilicode (CCA) -> text_code (Django) -> id
    COALESCE(
        (SELECT id FROM {schema}.gc_dominioconstrucciontipo
         WHERE text_code = cu.tipo_dominio LIMIT 1),
        (SELECT id FROM {schema}.gc_dominioconstrucciontipo
         WHERE text_code ILIKE '%' || cu.tipo_dominio || '%' LIMIT 1),
        NULL
    ),

    -- tipo_planta: Mapeo ilicode (CCA) -> text_code (Django) -> id
    COALESCE(
        (SELECT id FROM {schema}.gc_construccionplantatipo
         WHERE text_code = cu.tipo_planta LIMIT 1),
        (SELECT id FROM {schema}.gc_construccionplantatipo
         WHERE text_code ILIKE '%' || cu.tipo_planta || '%' LIMIT 1),
        NULL
    ),

    -- tipo_unidad_construccion: Mapeo ilicode (CCA) -> text_code (Django) -> id
    COALESCE(
        (SELECT id FROM {schema}.gc_unidadconstrucciontipo
         WHERE text_code = cu.tipo_unidad_construccion LIMIT 1),
        (SELECT id FROM {schema}.gc_unidadconstrucciontipo
         WHERE text_code ILIKE '%' || cu.tipo_unidad_construccion || '%' LIMIT 1),
        NULL
    ),

    -- uso: Mapeo ilicode (CCA) -> text_code (Django) -> id
    COALESCE(
        (SELECT id FROM {schema}.gc_usouconstipo
         WHERE text_code = cu.uso LIMIT 1),
        (SELECT id FROM {schema}.gc_usouconstipo
         WHERE text_code ILIKE '%' || cu.uso || '%' LIMIT 1),
        NULL
    )

FROM tmp_cca_caracteristicasunidadconstruccion cu;
