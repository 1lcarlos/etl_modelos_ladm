SELECT 
u.espacio_de_nombres, 
u.local_id, 
cn.text_code as tipo_planta,
u.comienzo_vida_util_version, 
u.fin_vida_util_version, 
u.codigo, 
u.etiqueta, 
u.observacion, 
u.geometria, u.id as local_id, u.planta_ubicacion, u.altura, 
u.gc_caracteristicasunidadconstruccion as cr_caracteristicasunidadconstruccion
FROM {schema}.gc_unidadconstruccion u 
LEFT JOIN {schema}.gc_caracteristicasunidadconstruccion c ON u.gc_caracteristicasunidadconstruccion = c.id
JOIN {schema}.gc_construccionplantatipo cn ON c.tipo_planta = cn.id;