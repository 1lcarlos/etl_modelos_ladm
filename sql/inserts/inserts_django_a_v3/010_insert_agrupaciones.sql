INSERT INTO {schema}.gc_agrupacioninteresados
( t_id
, t_basket
, t_ili_tid
, tipo
, nombre
, comienzo_vida_util_version
, espacio_de_nombres
, local_id)
SELECT 
 ai.id_agrupacion::bigint
,(SELECT t_id FROM {schema}.t_ili2db_basket LIMIT 1)
,uuid_generate_v4()
,(SELECT t_id FROM {schema}.col_grupointeresadotipo WHERE ilicode = ai.tipo_agrupacion LIMIT 1)
,nombre
,comienzo_vida_util_version::timestamp
,espacio_de_nombres
,coalesce(ai.id_agrupacion::varchar, ai.local_id::varchar) as local_id
FROM tmp_agrupacion_interesados ai





