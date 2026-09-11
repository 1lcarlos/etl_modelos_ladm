INSERT INTO {schema}.gc_puntolindero (
    t_id,
    t_basket,
t_ili_tid,
    espacio_de_nombres,
    local_id,
    comienzo_vida_util_version,
    fin_vida_util_version,
    id_punto_lindero,
    posicion_interpolacion,
    exactitud_horizontal,
    exactitud_vertical,
    acuerdo,
    fotoidentificacion,
    metodo_produccion,
    punto_tipo
)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    (SELECT t_id FROM {schema}.t_ili2db_basket WHERE topic = 'Modelo_Interno_ACC_V3.Modelo_Interno' LIMIT 1),
    uuid_generate_v4(),
    tpl.espacio_de_nombres,
    tpl.id::text,
    tpl.comienzo_vida_util_version::timestamp,
    tpl.fin_vida_util_version::timestamp,
    tpl.id_punto,
    tpl.geometria,
    tpl.exactitud_horizontal::numeric,
    tpl.exactitud_vertical::numeric,
    at.t_id,
    fit.t_id,
    mpt.t_id,
    pt.t_id
FROM tmp_gc_puntolindero tpl
LEFT JOIN {schema}.gc_acuerdotipo at ON tpl.acuerdo = at.ilicode
LEFT JOIN {schema}.gc_fotoidentificaciontipo fit ON tpl.fotoidentificacion = fit.ilicode
LEFT JOIN {schema}.col_metodoproducciontipo mpt ON tpl.metodo_produccion = mpt.ilicode
LEFT JOIN {schema}.gc_puntotipo pt ON tpl.punto_tipo = pt.ilicode;
