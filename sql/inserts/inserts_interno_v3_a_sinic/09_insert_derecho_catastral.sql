INSERT INTO {schema}.sinic_derechocatastral
(t_id
, t_basket
, t_ili_tid
, tipo
, descripcion
, interesado_cr_interesado
, interesado_cr_agrupacioninteresados
, unidad
, comienzo_vida_util_version
, espacio_de_nombres
, local_id)
SELECT
    d.id_derecho::bigint,
    (SELECT t_id FROM {schema}.t_ili2db_basket LIMIT 1),
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
        WHEN d.id_interesado IS NOT NULL
             AND EXISTS (SELECT 1 FROM {schema}.cr_interesado i WHERE i.t_id = d.id_interesado::numeric)
        THEN d.id_interesado::numeric
        ELSE NULL
    END,

    -- interesado_cr_agrupacioninteresados: referencia a agrupacion
    CASE
        WHEN d.id_agrupacioninteresados IS NOT NULL
             AND EXISTS (SELECT 1 FROM {schema}.cr_agrupacioninteresados ai WHERE ai.t_id = d.id_agrupacioninteresados::numeric)
        THEN d.id_agrupacioninteresados::numeric
        ELSE NULL
    END,

    -- unidad: referencia al predio
    CASE
        WHEN d.id_predio IS NOT NULL
             AND EXISTS (SELECT 1 FROM {schema}.sinic_predio p WHERE p.t_id = d.id_predio)
        THEN d.id_predio
        ELSE NULL
    END,
    
    -- comienzo_vida_util_version (NOT NULL)
    COALESCE(d.comienzo_vida_util_version::timestamp, NOW()),
    
    -- espacio_de_nombres (NOT NULL)
    COALESCE(d.espacio_de_nombres, 'SINIC_DERECHO'),

    -- local_id (NOT NULL)
    COALESCE(d.local_id, id_derecho::varchar)

FROM tmp_derecho_catastral d;