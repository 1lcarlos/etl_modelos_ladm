SELECT
    distinct on (gc_predio_avaluo)
    --seq,
    avaluo_catastral,
    vigencia,
    por_decreto::boolean,
    descripcion,
    gc_predio_avaluo,
    e.id as id_avaluo
FROM {schema}.extavaluo e
LEFT JOIN {schema}.gc_predio gcp ON e.gc_predio_avaluo = gcp.id
--where vigencia in ('2026-01-01')
WHERE gcp.id IS NOT NULL  -- Solo registros con predio asociado

