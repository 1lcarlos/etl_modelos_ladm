INSERT INTO {schema}.gc_datosmatriz (
    t_id,
    t_ili_tid,
    area_total_terreno,
    area_total_terreno_privada,
    area_total_terreno_comun,
    area_total_construida,
    area_total_construida_privada,
    area_total_construida_comun,
    numero_torres,
    total_unidades_privadas,
    cr_predio
)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    uuid_generate_v4(),
    tmp.area_total_terreno,
    tmp.area_total_terreno_privada,
    tmp.area_total_terreno_comun,
    tmp.area_total_construida,
    tmp.area_total_construida_privada,
    tmp.area_total_construida_comun,
    tmp.numero_torres,
    tmp.total_unidades_privadas,
    p.t_id
FROM tmp_gc_datosmatriz tmp
JOIN {schema}.gc_predio p
    ON p.local_id::numeric = tmp.predio_local_id::numeric;
