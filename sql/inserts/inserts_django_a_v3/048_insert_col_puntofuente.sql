INSERT INTO {schema}.col_puntofuente (
    t_id,
    t_ili_tid,
    fuente_espacial,
    punto_gc_puntocontrol,
    punto_gc_puntolindero
)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    uuid_generate_v4()::varchar,
    fe.t_id,
    pc.t_id,
    pl.t_id
FROM tmp_col_puntofuente tmp
JOIN {schema}.gc_fuenteespacial fe
    ON fe.local_id::numeric = tmp.fuente_espacial_local_id::numeric
LEFT JOIN {schema}.gc_puntocontrol pc
    ON pc.local_id::numeric = tmp.puntocontrol_local_id::numeric
LEFT JOIN {schema}.gc_puntolindero pl
    ON pl.local_id::numeric = tmp.puntolindero_local_id::numeric
WHERE fe.t_id IS NOT NULL;
