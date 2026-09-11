SELECT
    seq,
    avaluo_catastral,
    vigencia,
    por_decreto::integer,
    descripcion,
    gc_predio_avaluo,
    e.id
FROM {schema}.extavaluo e
LEFT JOIN {schema}.gc_predio gcp ON e.gc_predio_avaluo = gcp.id
--WHERE gc_predio_avaluo IS NOT NULL  -- Solo registros con predio asociado

