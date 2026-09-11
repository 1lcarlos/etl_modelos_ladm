INSERT INTO {schema}.cr_agrupacioninteresados
(t_id
, t_basket
, t_ili_tid
, tipo
, nombre
, comienzo_vida_util_version
, espacio_de_nombres
, local_id)
SELECT
    ai.id_agrupacion::bigint,
    (SELECT t_id FROM {schema}.t_ili2db_basket LIMIT 1),
    uuid_generate_v4(),

    -- tipo: Mapeo a col_grupointeresadotipo (NOT NULL)
    COALESCE(
        (SELECT t_id FROM {schema}.col_grupointeresadotipo WHERE ilicode = ai.tipo_agrupacion LIMIT 1),
        (SELECT t_id FROM {schema}.col_grupointeresadotipo WHERE ilicode = 'Grupo_Civil' LIMIT 1)
    ),

    -- nombre
    ai.nombre,
   
    -- comienzo_vida_util_version (NOT NULL)
    COALESCE(ai.comienzo_vida_util_version::timestamp, NOW()),   

    -- espacio_de_nombres (NOT NULL)
    COALESCE(ai.espacio_de_nombres, 'CR_AGRUPACION'),

    -- local_id (NOT NULL)
    COALESCE(ai.local_id, ai.id_agrupacion::varchar)

FROM tmp_agrupacion ai;