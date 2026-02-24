SELECT 
cc.id as id_calificacion_convencional,
cc.total_calificacion, 
ca.text_code as tipo_calificar, 
cu.id as gc_caracteristica_uconstruccion
FROM {schema}.cuc_calificacionconvencional cc
JOIN {schema}.cuc_calificartipo ca ON ca.id = cc.tipo_calificar
JOIN {schema}.gc_caracteristicasunidadconstruccion cu ON cu.id = cc.gc_caracteristicasunidadconstruccion;