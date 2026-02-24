-- INSERT para tabla ric_fuenteadministrativa
-- Migra fuentes administrativas desde la tabla temporal a la estructura RIC
-- Fecha: 2025-12-18
--
-- Dependencias:
--   - Usa datos de tmp_fuente_administrativa (query fuente_administrativa.sql)

INSERT INTO ric.ric_fuenteadministrativa (
    t_id,
    t_ili_tid,
    tipo,
    ente_emisor,
    oficina_origen,
    ciudad_origen,
    observacion,
    numero_fuente,
    estado_disponibilidad,
    tipo_principal,
    fecha_documento_fuente,
    espacio_de_nombres,
    local_id
)
SELECT
    --nextval('ric.t_ili2db_seq'::regclass),
    f.id::bigint,
    uuid_generate_v4(),

    -- tipo: Mapeo a col_fuenteadministrativatipo
    COALESCE(
        (SELECT t_id FROM ric.col_fuenteadministrativatipo fat
         WHERE fat.ilicode ILIKE '%' || f.tipo_fuente_administrativa || '%' 
         AND fat.baseclass is not null
         LIMIT 1),
        (SELECT t_id FROM ric.col_fuenteadministrativatipo fat
         WHERE fat.ilicode = 'Sin_Documento'
         AND fat.baseclass is not null
         LIMIT 1)
    ),

    -- ente_emisor
    f.ente_emisor,

    -- oficina_origen (integer 0-100)
    /* CASE
        WHEN f.ente_emisor ~ '^[0-9]+$' THEN
            LEAST(f.ente_emisor::integer, 100)
        ELSE 1
    END, */
    NULL,
    
    -- ciudad_origen
    NULL,

    -- observacion
    f.descripcion,

    -- numero_fuente
    f.numero_fuente,

    -- estado_disponibilidad (NOT NULL): Mapeo a col_estadodisponibilidadtipo
    COALESCE(
        (SELECT t_id FROM ric.col_estadodisponibilidadtipo
         WHERE ilicode ILIKE '%' || f.estado_disponibilidad || '%'
         LIMIT 1),
        (SELECT t_id FROM ric.col_estadodisponibilidadtipo
         WHERE ilicode = 'Desconocido'
         LIMIT 1)
    ),

    -- tipo_principal (puede ser NULL)
    NULL,

    -- fecha_documento_fuente
    f.fecha_documento_fuente::date,

    -- espacio_de_nombres (NOT NULL)
    COALESCE(f.espacio_de_nombres, 'RIC_FUENTE'),

    -- local_id (NOT NULL)
    COALESCE(f.id::varchar, f.local_id)

FROM tmp_fuente_administrativa f;
