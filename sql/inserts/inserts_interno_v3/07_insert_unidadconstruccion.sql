INSERT INTO {schema}.gc_unidadconstruccion
(t_id, t_ili_tid, tipo_planta, planta_ubicacion, 
altura, geometria, codigo, cr_caracteristicasunidadconstruccion, 
etiqueta, relacion_superficie, comienzo_vida_util_version, fin_vida_util_version, espacio_de_nombres, local_id)
SELECT 
    nextval('{schema}.t_ili2db_seq'::regclass),  
    uuid_generate_v4(),
    cpt.t_id as tipo_planta,
    u.planta_ubicacion,
    u.altura::numeric,
    u.geometria, 
    u.codigo,
    c.t_id as cr_caracteristicasunidadconstruccion,
    u.etiqueta,
  NULL as relacion_superficie,
  COALESCE(u.comienzo_vida_util_version::timestamp, CURRENT_TIMESTAMP) as comienzo_vida_util_version,
  COALESCE(u.fin_vida_util_version::timestamp, CURRENT_TIMESTAMP) as fin_vida_util_version,
  'gc_unidadconstruccion' as espacio_de_nombres, 
   COALESCE(u.id::varchar, local_id ) 
FROM tmp_unidadconstruccion u
join {schema}.gc_construccionplantatipo cpt on u.tipo_planta = cpt.ilicode
JOIN {schema}.gc_caracteristicasunidadconstruccion c ON u.cr_caracteristicasunidadconstruccion = c.t_id;