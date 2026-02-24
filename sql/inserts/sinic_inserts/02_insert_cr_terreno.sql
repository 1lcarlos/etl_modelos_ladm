-- INSERT para tabla cr_terreno
-- Migra terrenos desde la tabla temporal a SINIC
-- Fecha: 2026-02-02
--
-- Dependencias:
--   - Usa datos de tmp_sinic_terreno (query cr_terreno.sql)

INSERT INTO {schema}.cr_terreno (
    t_id,
    --t_basket,
    t_ili_tid,
    geometria,
    etiqueta,
    relacion_superficie,
    comienzo_vida_util_version,
    fin_vida_util_version,
    espacio_de_nombres,
    local_id
)
SELECT
    t.id::bigint,
    --(SELECT t_id FROM {schema}.t_ili2db_basket LIMIT 1),
    uuid_generate_v4(),

    -- geometria (NOT NULL)
    t.geometria,

    -- etiqueta
    t.etiqueta,

    -- relacion_superficie: Mapeo a col_relacionsuperficietipo
    COALESCE(
        (SELECT t_id FROM {schema}.col_relacionsuperficietipo WHERE ilicode = t.relacion_superficie LIMIT 1),
        (SELECT t_id FROM {schema}.col_relacionsuperficietipo WHERE ilicode = 'En_Rasante' LIMIT 1)
    ),

    -- comienzo_vida_util_version (NOT NULL)
    COALESCE(t.comienzo_vida_util_version::timestamp, NOW()),

    -- fin_vida_util_version (puede ser NULL)
    t.fin_vida_util_version::timestamp,

    -- espacio_de_nombres (NOT NULL)
    COALESCE(t.espacio_de_nombres, 'CR_TERRENO'),

    -- local_id (NOT NULL)
    COALESCE(t.local_id, t.id::varchar)

FROM tmp_cr_terreno t
WHERE t.geometria IS NOT NULL;
