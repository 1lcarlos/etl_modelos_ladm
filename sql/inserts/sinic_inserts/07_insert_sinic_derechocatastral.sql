-- INSERT para tabla sinic_derechocatastral
-- Migra derechos catastrales desde la tabla temporal a SINIC
-- Fecha: 2026-02-02
--
-- Dependencias:
--   - Requiere que sinic_predio, cr_interesado y cr_agrupacioninteresados ya esten migrados
--   - Usa datos de tmp_sinic_derechocatastral (query sinic_derechocatastral.sql)

INSERT INTO {schema}.sinic_derechocatastral (
    t_id,
    --t_basket,
    t_ili_tid,
    tipo,
    descripcion,
    interesado_cr_interesado,
    interesado_cr_agrupacioninteresados,
    unidad,
    comienzo_vida_util_version,
    fin_vida_util_version,
    espacio_de_nombres,
    local_id
)
SELECT
    d.id::bigint,
    --(SELECT t_id FROM {schema}.t_ili2db_basket LIMIT 1),
    uuid_generate_v4(),

    -- tipo: Mapeo a sinic_derechocatastraltipo (NOT NULL)
    COALESCE(
        (SELECT t_id FROM {schema}.sinic_derechocatastraltipo WHERE ilicode = d.tipo_derecho LIMIT 1),
        (SELECT t_id FROM {schema}.sinic_derechocatastraltipo WHERE ilicode = 'Dominio' LIMIT 1)
    ),

    -- descripcion
    d.descripcion,

    -- interesado_cr_interesado: referencia al interesado
    CASE
        WHEN d.interesado_id IS NOT NULL
             AND EXISTS (SELECT 1 FROM {schema}.cr_interesado i WHERE i.t_id = d.interesado_id::numeric)
        THEN d.interesado_id::numeric
        ELSE NULL
    END,

    -- interesado_cr_agrupacioninteresados: referencia a agrupacion
    CASE
        WHEN d.agrupacion_id IS NOT NULL
             AND EXISTS (SELECT 1 FROM {schema}.cr_agrupacioninteresados ai WHERE ai.t_id = d.agrupacion_id::numeric)
        THEN d.agrupacion_id::numeric
        ELSE NULL
    END,

    -- unidad: referencia al predio
    CASE
        WHEN d.predio_id IS NOT NULL
             AND EXISTS (SELECT 1 FROM {schema}.sinic_predio p WHERE p.t_id = d.predio_id)
        THEN d.predio_id
        ELSE NULL
    END,

    -- comienzo_vida_util_version (NOT NULL)
    COALESCE(d.comienzo_vida_util_version::timestamp, NOW()),

    -- fin_vida_util_version (puede ser NULL)
    d.fin_vida_util_version::timestamp,

    -- espacio_de_nombres (NOT NULL)
    COALESCE(d.espacio_de_nombres, 'SINIC_DERECHO'),

    -- local_id (NOT NULL)
    COALESCE(d.local_id, d.id::varchar)

FROM tmp_sinic_derechocatastral d;
