INSERT INTO {schema}.col_masccl (
    t_id,
    ccl_mas,
    ue_mas_cr_terreno,
    ue_mas_cr_unidadconstruccion
)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    l.t_id,
    t.t_id,
    uc.t_id
FROM tmp_col_masccl tm
JOIN {schema}.cr_lindero l ON tm.id_lindero::text = l.local_id
JOIN {schema}.cr_terreno t ON tm.id_terreno::text = t.local_id
LEFT JOIN {schema}.cr_unidadconstruccion uc ON tm.id_unidad_construccion::text = uc.local_id;
