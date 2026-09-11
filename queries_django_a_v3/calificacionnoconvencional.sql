SELECT 
ca.text_code as tipo_anexo, 
cu.id as gc_caracteristica_uconstruccion
FROM {schema}.cuc_calificacionnoconvencional cc
JOIN {schema}.cuc_anexotipo ca ON ca.id = cc.tipo_anexo
JOIN {schema}.gc_caracteristicasunidadconstruccion cu ON cu.id = cc.gc_caracteristicasunidadconstruccion;