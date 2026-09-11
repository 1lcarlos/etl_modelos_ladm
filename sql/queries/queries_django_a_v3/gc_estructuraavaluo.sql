SELECT 
  av.id as id_avaluo
, '2026-01-01'::date as fecha_avaluo
, av.avaluo_catastral
, false::boolean as autoestimacion
, p.id  as id_predio
, av.vigencia
FROM {schema}.extavaluo av
join {schema}.gc_predio p on av.gc_predio_avaluo = p.id 
where av.vigencia = '2026-01-01';
