-- INSERT para tabla cr_fuenteadministrativa
-- Migra fuentes administrativas desde la tabla temporal a SINIC
-- Fecha: 2026-02-02
--
-- Dependencias:
--   - Usa datos de tmp_sinic_fuenteadministrativa (query cr_fuenteadministrativa.sql)

INSERT INTO {schema}.cr_fuenteadministrativa (
    t_id,
    --t_basket,
    t_ili_tid,
    tipo,
    --numero_fuente,
    fecha_documento_fuente,
    estado_disponibilidad,
    --tipo_principal,
    ente_emisor,
    --comienzo_vida_util_version,
    --fin_vida_util_version,
    espacio_de_nombres,
    local_id
)
SELECT
    fa.id::bigint,
    --(SELECT t_id FROM {schema}.t_ili2db_basket LIMIT 1),
    uuid_generate_v4(),

    -- tipo: Mapeo a col_fuenteadministrativatipo
    COALESCE(
        (SELECT t_id FROM {schema}.col_fuenteadministrativatipo WHERE ilicode = fa.tipo_fuente and baseclass is not null LIMIT 1),
        (SELECT t_id FROM {schema}.col_fuenteadministrativatipo WHERE ilicode = 'Sin_Documento' and baseclass is not null LIMIT 1)
    ),

    -- numero_fuente
    --fa.numero_fuente,

    -- fecha_documento_fuente
    fa.fecha_documento_fuente::date,

    -- estado_disponibilidad: Mapeo a col_estadodisponibilidadtipo
    COALESCE(
        (SELECT t_id FROM {schema}.col_estadodisponibilidadtipo WHERE ilicode = fa.estado_disponibilidad LIMIT 1),
        (SELECT t_id FROM {schema}.col_estadodisponibilidadtipo WHERE ilicode = 'Disponible' LIMIT 1)
    ),

    -- tipo_principal (puede ser NULL - ajustar segun modelo destino)
    --NULL,

    -- ente_emisor
    fa.ente_emisor,

    -- comienzo_vida_util_version (NOT NULL - usar valor por defecto)
    --NOW(),

    -- fin_vida_util_version (puede ser NULL)
    --NULL,

    -- espacio_de_nombres (NOT NULL)
    COALESCE(fa.espacio_de_nombres, 'CR_FUENTEADM'),

    -- local_id (NOT NULL)
    COALESCE(fa.id::varchar, fa.local_id)

FROM tmp_cr_fuenteadministrativa fa;
