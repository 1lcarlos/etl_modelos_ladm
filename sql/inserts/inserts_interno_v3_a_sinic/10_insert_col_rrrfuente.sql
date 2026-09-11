INSERT INTO {schema}.col_rrrfuente
(t_id
, t_basket
, fuente_administrativa
, rrr)
SELECT
    rf.t_id::bigint,
    (SELECT t_id FROM {schema}.t_ili2db_basket LIMIT 1),

    -- fuente_espacial: referencia a la fuente administrativa
    CASE
        WHEN rf.id_fuente_administrativa IS NOT NULL
             AND EXISTS (SELECT 1 FROM {schema}.cr_fuenteadministrativa fa WHERE fa.t_id = rf.id_fuente_administrativa)
        THEN rf.id_fuente_administrativa
        ELSE NULL
    END,

    -- rrr_sinic_derechocatastral: referencia al derecho
    CASE
        WHEN rf.id_derecho IS NOT NULL
             AND EXISTS (SELECT 1 FROM {schema}.sinic_derechocatastral d WHERE d.t_id = rf.id_derecho)
        THEN rf.id_derecho
        ELSE NULL
    END

FROM tmp_col_rrrfuente rf
WHERE rf.id_derecho IS NOT NULL;