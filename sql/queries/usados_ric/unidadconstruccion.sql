SELECT 
u.id,
COALESCE(u.espacio_de_nombres, 'GC_UNIDADCONSTRUCCION') as espacio_de_nombres,
u.local_id, 
cn.text_code as tipo_planta,
u.comienzo_vida_util_version,
u.fin_vida_util_version, 
u.codigo, 
COALESCE(u.etiqueta, '0') as etiqueta, 
u.observacion, 
u.geometria, 
COALESCE(u.planta_ubicacion, 1) as planta_ubicacion, 
u.altura, 
u.gc_caracteristicasunidadconstruccion as cr_caracteristicasunidadconstruccion,
u.gc_construccion as cr_construccion
FROM {schema}.gc_unidadconstruccion u 
LEFT JOIN {schema}.gc_caracteristicasunidadconstruccion c ON u.gc_caracteristicasunidadconstruccion = c.id
JOIN {schema}.gc_construccionplantatipo cn ON c.tipo_planta = cn.id;
