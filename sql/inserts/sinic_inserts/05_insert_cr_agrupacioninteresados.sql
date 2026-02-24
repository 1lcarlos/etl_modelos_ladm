-- INSERT para tabla cr_agrupacioninteresados
-- Migra agrupaciones de interesados desde la tabla temporal a SINIC
-- Fecha: 2026-02-02
--
-- Dependencias:
--   - Usa datos de tmp_sinic_agrupacioninteresados (query cr_agrupacioninteresados.sql)

INSERT INTO {schema}.cr_agrupacioninteresados (
    t_id,
    --t_basket,
    t_ili_tid,
    tipo,
    nombre,
    tipo_interesado,
    tipo_documento,
    numero_documento,
    comienzo_vida_util_version,
    fin_vida_util_version,
    espacio_de_nombres,
    local_id
)
SELECT
    ai.id::bigint,
    --(SELECT t_id FROM {schema}.t_ili2db_basket LIMIT 1),
    uuid_generate_v4(),

    -- tipo: Mapeo a col_grupointeresadotipo (NOT NULL)
    COALESCE(
        (SELECT t_id FROM {schema}.col_grupointeresadotipo WHERE ilicode = ai.tipo_grupo LIMIT 1),
        (SELECT t_id FROM {schema}.col_grupointeresadotipo WHERE ilicode = 'Grupo_Civil' LIMIT 1)
    ),

    -- nombre
    ai.nombre,

    -- tipo_interesado: Mapeo a col_interesadotipo
    (SELECT t_id FROM {schema}.col_interesadotipo WHERE ilicode = ai.tipo_interesado LIMIT 1),

    -- tipo_documento: Mapeo a col_documentotipo
    (SELECT t_id FROM {schema}.col_documentotipo WHERE ilicode = ai.tipo_documento LIMIT 1),

    -- numero_documento
    ai.numero_documento,

    -- comienzo_vida_util_version (NOT NULL)
    COALESCE(ai.comienzo_vida_util_version::timestamp, NOW()),

    -- fin_vida_util_version (puede ser NULL)
    ai.fin_vida_util_version::timestamp,

    -- espacio_de_nombres (NOT NULL)
    COALESCE(ai.espacio_de_nombres, 'CR_AGRUPACION'),

    -- local_id (NOT NULL)
    COALESCE(ai.local_id, ai.id::varchar)

FROM tmp_cr_agrupacioninteresados ai;
