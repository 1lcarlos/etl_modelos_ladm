INSERT INTO {schema}.col_cclfuente (
    t_id,
    t_ili_tid,
    ccl,
    fuente_espacial
)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    uuid_generate_v4()::varchar,
    l.t_id,
    fe.t_id
FROM tmp_col_cclfuente tmp
JOIN {schema}.gc_lindero l
    ON l.local_id::numeric = tmp.lindero_local_id::numeric
JOIN {schema}.gc_fuenteespacial fe
    ON fe.local_id::numeric = tmp.fuente_espacial_local_id::numeric;
