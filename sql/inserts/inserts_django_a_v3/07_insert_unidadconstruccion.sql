INSERT INTO {schema}.gc_unidadconstruccion
(t_id,
t_basket,
t_ili_tid, 
tipo_planta, 
planta_ubicacion, 
altura, 
geometria, 
codigo, 
cr_caracteristicasunidadconstruccion, 
etiqueta, 
relacion_superficie, 
comienzo_vida_util_version, 
fin_vida_util_version, 
espacio_de_nombres, 
local_id)
SELECT 
    u.id_unidadconstruccion::bigint,
    (SELECT t_id FROM {schema}.t_ili2db_basket LIMIT 1),
    uuid_generate_v4(),
    COALESCE(
        (SELECT t_id FROM {schema}.gc_construccionplantatipo WHERE ilicode ILIKE '%' || u.tipo_planta || '%' ORDER BY t_id LIMIT 1),
        (SELECT t_id FROM {schema}.gc_construccionplantatipo ORDER BY t_id LIMIT 1)
    ) AS tipo_planta,
    COALESCE(u.planta_ubicacion::integer, 0),
    u.altura::numeric,
    u.geometria, 
    u.codigo,
    u.id_caracteristicasunidadconstruccion::bigint,
    u.etiqueta,
  NULL as relacion_superficie,
  COALESCE(u.comienzo_vida_util_version::timestamp, CURRENT_TIMESTAMP) as comienzo_vida_util_version,
  COALESCE(u.fin_vida_util_version::timestamp, CURRENT_TIMESTAMP) as fin_vida_util_version,
  'gc_unidadconstruccion' as espacio_de_nombres, 
   COALESCE(u.id_unidadconstruccion::varchar, u.local_id::varchar) as local_id 
FROM tmp_unidadconstruccion u
where u.id_caracteristicasunidadconstruccion in (select t_id from {schema}.gc_caracteristicasunidadconstruccion);
