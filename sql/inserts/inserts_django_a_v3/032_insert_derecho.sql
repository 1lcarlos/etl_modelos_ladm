INSERT INTO
    {schema}.gc_derechocatastral (
        t_id,
        t_basket,
        t_ili_tid,
        tipo,
        descripcion,
        interesado_gc_interesado,
        interesado_gc_agrupacioninteresados,
        unidad,
        comienzo_vida_util_version,
        fin_vida_util_version,
        espacio_de_nombres,
        local_id
    )
SELECT
    tmpd.id_derecho::bigint,
    (SELECT t_id FROM {schema}.t_ili2db_basket LIMIT 1),
    uuid_generate_v4(),
    COALESCE(
        (SELECT t_id FROM {schema}.gc_derechocatastraltipo WHERE ilicode ILIKE '%' || tmpd.tipo_derecho || '%' ORDER BY t_id LIMIT 1),
        (SELECT t_id FROM {schema}.gc_derechocatastraltipo ORDER BY t_id LIMIT 1)
    ),
    tmpd.descripcion,
    tmpd.id_interesado::numeric,
    tmpd.id_agrupacion::numeric,
    tmpd.id_predio::numeric,
    tmpd.comienzo_vida_util_version::timestamp,
    tmpd.fin_vida_util_version::timestamp,
    tmpd.espacio_de_nombres,
    tmpd.id_derecho::varchar
FROM
    tmp_derecho tmpd
    where tmpd.id_interesado::numeric  IN (SELECT t_id FROM {schema}.gc_interesado) or tmpd.id_agrupacion::numeric IN (SELECT t_id FROM {schema}.gc_agrupacioninteresados) or tmpd.id_predio::numeric IN (SELECT t_id FROM {schema}.gc_predio); 