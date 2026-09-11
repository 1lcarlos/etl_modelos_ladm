SELECT 
av.t_id as id_avaluo
,  av.fecha_avaluo
, av.avaluo_catastral
, false::boolean as autoestimacion
, av.gc_predio_avaluo  as id_predio
FROM {schema}.gc_estructuraavaluo av
JOIN {schema}.gc_predio_tramitecatastral ptc ON av.gc_predio_avaluo  = ptc.cr_predio
where av.vigencia = '2026-01-01';