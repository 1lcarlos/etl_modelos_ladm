INSERT INTO {schema}.col_miembros
(t_id,
t_basket
, interesado_gc_interesado
, agrupacion
, participacion)
SELECT 
m.id_miembros
,(SELECT t_id FROM {schema}.t_ili2db_basket LIMIT 1)
,m.id_interesado
,m.id_agrupacion
,m.participacion::numeric
FROM tmp_col_miembros m
WHERE m.id_interesado IN (SELECT t_id FROM {schema}.gc_interesado) and m.id_agrupacion IN (SELECT t_id FROM {schema}.gc_agrupacioninteresados);