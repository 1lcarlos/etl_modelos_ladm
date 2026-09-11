-- Vincula linderos (CCL) con fuentes espaciales
SELECT
    l.local_id   AS lindero_local_id,
    fe.local_id  AS fuente_espacial_local_id
FROM {schema}.col_cclfuente ccl
JOIN {schema}.gc_lindero l         ON l.id  = ccl.ccl
JOIN {schema}.gc_fuenteespacial fe ON fe.id = ccl.fuente_espacial;
