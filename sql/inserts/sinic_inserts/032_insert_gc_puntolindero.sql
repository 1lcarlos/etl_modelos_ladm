INSERT INTO {schema}.cr_puntolindero (
    t_id,
    t_ili_tid,
    espacio_de_nombres,
    local_id,
    comienzo_vida_util_version,
    fin_vida_util_version,
    id_punto_lindero,
    geometria,
    --exactitud_horizontal,
    fotoidentificacion,
    metodoproduccion,
    puntotipo,
    desacuerdo
)
SELECT
    --nextval('{schema}.t_ili2db_seq'::regclass),
    tpl.id_puntolindero,
    uuid_generate_v4(),
    tpl.espacio_de_nombres,
    tpl.id_puntolindero::text,
    tpl.comienzo_vida_util_version::timestamp,
    tpl.fin_vida_util_version::timestamp,
    tpl.id_punto,
    tpl.geometria,
    --tpl.exactitud_horizontal::numeric,  
    fit.t_id,
    coalesce((SELECT t_id FROM {schema}.col_metodoproducciontipo mpt WHERE mpt.ilicode = tpl.metodo_produccion LIMIT 1), (SELECT t_id FROM {schema}.col_metodoproducciontipo mpt WHERE mpt.ilicode = 'Metodo_Indirecto' LIMIT 1)) as metodoproducciontipo_t_id,
    --mpt.t_id,
    coalesce((SELECT t_id FROM {schema}.col_puntotipo pt WHERE pt.ilicode ILIKE '%' || tpl.punto_tipo || '%' and pt.baseclass is not null LIMIT 1), (SELECT t_id FROM {schema}.col_puntotipo pt WHERE pt.ilicode = 'Catastro.Sin_Materializacion' and pt.baseclass is not null LIMIT 1)) as puntotipo_t_id,
    --pt.t_id,
    coalesce(tpl.desacuerdo::boolean, false::boolean) as desacuerdo
    --tpl.desacuerdo::boolean
FROM tmp_gc_puntolindero tpl
LEFT JOIN {schema}.cr_fotoidentificaciontipo fit ON tpl.fotoidentificacion = fit.ilicode
--LEFT JOIN {schema}.col_metodoproducciontipo mpt ON tpl.metodo_produccion = mpt.ilicode
--LEFT JOIN {schema}.col_puntotipo pt ON pt.ilicode ILIKE '%' || tpl.punto_tipo || '%'
--WHERE pt.baseclass is not null