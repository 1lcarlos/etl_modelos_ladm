SELECT  
ue.id as id_col_uebaunit,
u.id as id_unidadconstruccion,
ue.unidad as id_predio
FROM {schema}.col_uebaunit ue
JOIN {schema}.gc_unidadconstruccion u ON ue.ue_gc_unidadconstruccion = u.id;


