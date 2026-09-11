INSERT INTO {schema}.col_uebaunit (
    t_id,
    t_basket,
    ue_gc_terreno,
    baunit
)
SELECT
id_col_uebaunit,
(select t_id from {schema}.t_ili2db_basket limit 1),
ue.id_terreno,
ue.unidad
FROM tmp_col_uebaunit_predio_terreno ue
where ue.id_terreno in (select t_id from {schema}.gc_terreno);