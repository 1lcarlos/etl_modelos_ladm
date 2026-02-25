INSERT INTO {schema}.col_menosccl (
    t_id,
    ccl_menos,
    ue_menos_cr_terreno,
    ue_menos_cr_unidadconstruccion
)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    l.t_id,
    t.t_id,
    uc.t_id
FROM tmp_col_menosccl tm
JOIN {schema}.cr_lindero l ON tm.ccl_menos::text = l.local_id
JOIN {schema}.cr_terreno t ON tm.ue_menos_gc_terreno::text = t.local_id
LEFT JOIN {schema}.cr_unidadconstruccion uc ON tm.ue_menos_gc_unidadconstruccion::text = uc.local_id;
