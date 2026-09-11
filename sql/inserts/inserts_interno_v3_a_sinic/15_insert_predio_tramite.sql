INSERT INTO {schema}.cr_predio_tramitecatastral
(t_id
, t_basket
, cr_predio
, cr_tramite_catastral)
SELECT 
t.id_predio_tramite
, (SELECT t_id FROM {schema}.t_ili2db_basket LIMIT 1)
,  t.id_predio
,  t.id_tramite_catastral
FROM tmp_predio_tramite t;