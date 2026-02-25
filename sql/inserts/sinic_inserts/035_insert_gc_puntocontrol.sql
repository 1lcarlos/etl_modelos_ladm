-- Insert para la tabla gc_puntocontrol
INSERT INTO {schema}.cr_puntocontrol (
    t_id,
    t_ili_tid,
    espacio_de_nombres,
    local_id,
    comienzo_vida_util_version,
    fin_vida_util_version,
    id_punto_control,
    geometria,
    exactitud_horizontal,
    --exactitud_vertical,
    metodoproduccion,
    puntotipo,
    tipo_punto_control
)
SELECT
    --nextval('{schema}.t_ili2db_seq'::regclass),
    tpc.id_punto_control,
    uuid_generate_v4(),
    tpc.espacio_de_nombres,
    tpc.id_punto_control::text,
    tpc.comienzo_vida_util_version::timestamp,
    tpc.fin_vida_util_version::timestamp,
    tpc.id_punto,
    tpc.geometria,
    tpc.exactitud_horizontal::numeric,
    --tpc.exactitud_vertical::numeric,
    mpt.t_id,
    pt.t_id,
    pct.t_id
FROM tmp_gc_puntocontrol tpc
JOIN {schema}.col_metodoproducciontipo mpt ON tpc.metodo_produccion = mpt.ilicode
JOIN {schema}.col_puntotipo pt ON tpc.punto_tipo = pt.ilicode
JOIN {schema}.cr_puntocontroltipo pct ON tpc.tipo_punto_control = pct.ilicode;
