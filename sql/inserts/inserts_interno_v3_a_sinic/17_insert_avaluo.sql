INSERT INTO {schema}.cr_estructuraavaluo
(t_id
, t_basket
, t_ili_tid
,  fecha_avaluo
,  avaluo_catastral
,  autoestimacion
,  sinic_predio_avaluo)
SELECT 
id_avaluo
,(SELECT t_id FROM {schema}.t_ili2db_basket LIMIT 1)
,uuid_generate_v4()
,fecha_avaluo::date
,avaluo_catastral::numeric(16,1)
,autoestimacion::boolean
,id_predio::integer
FROM tmp_avaluo;