INSERT INTO {schema}.col_uebaunit (
    t_id,
    ue_gc_unidadconstruccion,
    baunit
)
SELECT
 nextval('{schema}.t_ili2db_seq'::regclass),
gu.t_id,  
p.t_id
FROM tmp_col_uebaunit_predio_unidadconstruccion tmp
join {schema}.gc_unidadconstruccion gu on tmp.ue_gc_unidadconstruccion = gu.local_id::numeric
join {schema}.gc_predio p on tmp.baunit = p.local_id::numeric;

