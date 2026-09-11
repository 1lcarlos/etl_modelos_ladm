SELECT 
 t.t_id as id_tramite
, t_ili_tid
, mt.ilicode as clasificacion_mutacion
, numero_resolucion
, fecha_resolucion
, fecha_radicacion
, fecha_inscripcion
FROM {schema}.gc_tramitecatastral t
join {schema}.gc_mutaciontipo mt on t.clasificacion_mutacion = mt.t_id  ;