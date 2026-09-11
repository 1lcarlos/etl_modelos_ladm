INSERT INTO {schema}.gc_lindero (
    t_id,
    t_basket,
t_ili_tid,
    espacio_de_nombres,
    local_id,
    comienzo_vida_util_version,
    fin_vida_util_version,
    localizacion_textual,
    geometria,
    longitud
)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    (SELECT t_id FROM {schema}.t_ili2db_basket WHERE topic = 'Modelo_Interno_ACC_V3.Modelo_Interno' LIMIT 1),
    uuid_generate_v4(),
    tl.espacio_de_nombres,
    tl.id::text,
    tl.comienzo_vida_util_version::timestamp,
    tl.fin_vida_util_version::timestamp,
    tl.localizacion_textual,
    tl.geometria,
    tl.longitud::numeric
FROM tmp_gc_lindero tl;
