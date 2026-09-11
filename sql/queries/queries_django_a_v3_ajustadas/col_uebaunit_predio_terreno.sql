SELECT  
t.id as ue_gc_terreno,
unidad

FROM {schema}.col_uebaunit cu
JOIN {schema}.gc_terreno t ON cu.ue_gc_terreno = t.id;