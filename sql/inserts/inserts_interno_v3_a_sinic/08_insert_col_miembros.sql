INSERT INTO {schema}.col_miembros
(t_id
, t_basket
, interesado_cr_interesado
,  agrupacion)
SELECT
    m.t_id::bigint,
    (SELECT t_id FROM {schema}.t_ili2db_basket LIMIT 1),

    -- interesado_cr_interesado: referencia al interesado migrado
    m.interesado_id,    

    -- agrupacion: referencia a la agrupacion (NOT NULL)
    m.agrupacion_id


FROM tmp_col_miembros m
WHERE m.agrupacion_id IS NOT NULL
  AND EXISTS (SELECT 1 FROM {schema}.cr_agrupacioninteresados ai WHERE ai.t_id = m.agrupacion_id);