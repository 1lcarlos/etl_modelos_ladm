-- Nota: area_terreno_interseccion es NOT NULL en destino;
--       como no existe en Django se usa 0 como valor por defecto.
INSERT INTO {schema}.gc_predio_informalidad (
    t_id,
    t_ili_tid,
    cr_predio_formal,
    cr_predio_informal,
    area_terreno_interseccion,
    area_construida_interseccion
)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    uuid_generate_v4()::varchar,
    pf.t_id,
    pi.t_id,
    0,
    NULL
FROM tmp_gc_predio_informalidad tmp
JOIN {schema}.gc_predio pf ON pf.local_id::numeric = tmp.predio_formal_local_id::numeric
JOIN {schema}.gc_predio pi ON pi.local_id::numeric = tmp.predio_informal_local_id::numeric;
