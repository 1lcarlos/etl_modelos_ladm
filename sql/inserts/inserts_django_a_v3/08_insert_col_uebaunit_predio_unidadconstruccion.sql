INSERT INTO {schema}.col_uebaunit (
    t_id,
    t_basket,
    ue_gc_unidadconstruccion,
    baunit
)
SELECT
 ue.id_col_uebaunit,
(SELECT t_id FROM {schema}.t_ili2db_basket LIMIT 1),
ue.id_unidadconstruccion,  
ue.id_predio
FROM tmp_col_uebaunit_predio_unidadconstruccion ue
where ue.id_unidadconstruccion in (select t_id from {schema}.gc_unidadconstruccion) and  ue.id_predio in (select t_id from {schema}.gc_predio);

