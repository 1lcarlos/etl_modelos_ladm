-- INSERT para tabla col_rrrfuente
-- Fecha: 2025-12-18

INSERT INTO ric.col_rrrfuente (t_id, fuente_administrativa, rrr)
SELECT
    nextval('ric.t_ili2db_seq'::regclass),
    (SELECT rf.t_id FROM ric.ric_fuenteadministrativa rf WHERE rf.local_id = r.fuente_administrativa::varchar LIMIT 1),
    (SELECT rd.t_id FROM ric.ric_derecho rd WHERE rd.local_id = r.rrr_gc_derecho::varchar LIMIT 1)
FROM tmp_col_rrrfuente r
WHERE r.fuente_administrativa IS NOT NULL AND r.rrr_gc_derecho IS NOT NULL
  AND EXISTS (SELECT 1 FROM ric.ric_fuenteadministrativa rf WHERE rf.local_id = r.fuente_administrativa::varchar)
  AND EXISTS (SELECT 1 FROM ric.ric_derecho rd WHERE rd.local_id = r.rrr_gc_derecho::varchar);
