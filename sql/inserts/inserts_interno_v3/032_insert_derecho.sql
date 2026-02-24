INSERT INTO
    {schema}.gc_derechocatastral (
        t_id,
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
    nextval('{schema}.t_ili2db_seq'::regclass),
    uuid_generate_v4(),
    dt.t_id,
    tmpd.descripcion,
    tmpd.interesado_gc_interesado::numeric,
    tmpd.interesado_gc_agrupacioninteresados::numeric,
    p.t_id::numeric,   
    tmpd.comienzo_vida_util_version::timestamp,
    tmpd.fin_vida_util_version::timestamp,
    tmpd.espacio_de_nombres,
    tmpd.id
FROM
    tmp_derecho tmpd 
    JOIN {schema}.gc_derechocatastraltipo dt ON dt.ilicode = tmpd.tipo_derecho
    LEFT JOIN {schema}.gc_predio p ON p.local_id::numeric = tmpd.baunit