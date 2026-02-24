INSERT INTO {schema}.col_miembros
(t_id
, interesado_gc_interesado
, agrupacion
, participacion)
SELECT 
nextval('{schema}.t_ili2db_seq'::regclass)
,interesado_gc_interesado
,ai.t_id as agrupacion
,participacion::numeric
FROM tmp_col_miembros tcm
JOIN (
    SELECT DISTINCT ON (local_id::numeric) t_id, local_id
    FROM {schema}.gc_agrupacioninteresados
) ai ON ai.local_id::numeric = tcm.agrupacion