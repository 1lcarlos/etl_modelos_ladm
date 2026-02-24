INSERT INTO {schema}.col_uebaunit (
    t_id,
    ue_gc_unidadconstruccion,
    baunit
)
SELECT
 nextval('{schema}.t_ili2db_seq'::regclass),
ue_gc_unidadconstruccion,  
 unidad as baunit
FROM tmp_col_uebaunit_predio_unidadconstruccion;