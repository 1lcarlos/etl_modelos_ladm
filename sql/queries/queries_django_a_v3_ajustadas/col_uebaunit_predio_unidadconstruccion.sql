SELECT  
gu.id as ue_gc_unidadconstruccion,
cu.unidad as baunit

FROM {schema}.col_uebaunit cu
JOIN {schema}.gc_unidadconstruccion gu ON cu.ue_gc_unidadconstruccion = gu.id;

-- pasaron en Manta -2 registros
