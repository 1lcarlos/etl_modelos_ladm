INSERT INTO {schema}.cr_tramitecatastral
(t_id
, t_basket
, t_ili_tid
, clasificacion_mutacion
, numero_resolucion
, fecha_resolucion
, fecha_radicacion
, fecha_inscripcion)
SELECT 
t.id_tramite::bigint
,(SELECT t_id FROM {schema}.t_ili2db_basket LIMIT 1)
,uuid_generate_v4()
,(SELECT t_id FROM {schema}.cr_mutaciontipo WHERE ilicode = t.clasificacion_mutacion LIMIT 1)
,numero_resolucion
,fecha_resolucion::date
,fecha_radicacion::date
, fecha_inscripcion::date
FROM tmp_tramite_catastral t;