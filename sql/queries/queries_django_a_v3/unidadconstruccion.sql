SELECT 
u.espacio_de_nombres, 
u.local_id, 
cn.text_code as tipo_planta,
u.comienzo_vida_util_version, 
u.fin_vida_util_version, 
u.codigo, 
u.etiqueta, 
u.observacion, 
u.geometria
, u.id as id_unidadconstruccion
, u.planta_ubicacion
, u.altura, 
u.gc_caracteristicasunidadconstruccion as id_caracteristicasunidadconstruccion
FROM {schema}.gc_unidadconstruccion u 
JOIN {schema}.gc_caracteristicasunidadconstruccion cc ON u.gc_caracteristicasunidadconstruccion = cc.id
JOIN {schema}.gc_construccionplantatipo cn ON cc.tipo_planta = cn.id
join {schema}.col_uebaunit ue on u.id = ue.ue_gc_unidadconstruccion
where u.geometria is not null;