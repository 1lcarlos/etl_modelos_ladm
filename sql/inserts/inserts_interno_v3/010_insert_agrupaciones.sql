INSERT INTO {schema}.gc_agrupacioninteresados
( t_id
, t_ili_tid
, tipo
, nombre
, comienzo_vida_util_version
, espacio_de_nombres
, local_id)
SELECT 
id_agrupacion
,uuid_generate_v4()
,git.t_id
,nombre
,comienzo_vida_util_version::timestamp
,espacio_de_nombres
,id_agrupacion
FROM tmp_agrupacion_interesados ai
join {schema}.col_grupointeresadotipo git on git.ilicode ILIKE '%' || ai.tipo_agrupacion || '%'


