SELECT  
ue.id as id_col_uebaunit,
t.id as id_terreno,
ue.unidad
FROM {schema}.col_uebaunit ue
JOIN {schema}.gc_terreno t ON ue.ue_gc_terreno = t.id;