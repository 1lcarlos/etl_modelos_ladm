INSERT INTO {schema}.gc_puntolindero (
    t_id,
    t_ili_tid,
    espacio_de_nombres,
    local_id,
    comienzo_vida_util_version,
    fin_vida_util_version,
    id_punto_lindero,
    geometria,
    exactitud_horizontal,
    fotoidentificacion,
    metodoproduccion,
    puntotipo,
    desacuerdo
)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    uuid_generate_v4(),
    tpl.espacio_de_nombres,
    tpl.id::text,
    tpl.comienzo_vida_util_version::timestamp,
    tpl.fin_vida_util_version::timestamp,
    tpl.id_punto,
    tpl.geometria,
    tpl.exactitud_horizontal::numeric,  
    fit.t_id,
    mpt.t_id,
    pt.t_id,
    desacuerdo
FROM tmp_gc_puntolindero tpl
JOIN {schema}.gc_fotoidentificaciontipo fit ON tpl.fotoidentificacion = fit.ilicode
JOIN {schema}.col_metodoproducciontipo mpt ON tpl.metodo_produccion = mpt.ilicode
JOIN {schema}.col_puntotipo pt ON pt.ilicode ILIKE '%' || tpl.punto_tipo || '%'
WHERE pt.baseclass is not null