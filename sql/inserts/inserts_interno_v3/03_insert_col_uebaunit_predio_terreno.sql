INSERT INTO {schema}.col_uebaunit (
    t_id,
    ue_gc_terreno,
    baunit
)
SELECT
nextval('{schema}.t_ili2db_seq'::regclass),
t.t_id,
p.t_id
FROM tmp_col_uebaunit_predio_terreno
join {schema}.gc_terreno t on tmp_col_uebaunit_predio_terreno.ue_gc_terreno = t.local_id::numeric
join {schema}.gc_predio p on tmp_col_uebaunit_predio_terreno.unidad = p.local_id::numeric;