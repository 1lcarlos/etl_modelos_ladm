-- INSERT para tabla ric_predio_copropiedad
-- Fecha: 2025-12-19

INSERT INTO ric.ric_predio_copropiedad (
  t_id, 
  ric_unidad_predial, 
  ric_matriz, 
  coeficiente)
SELECT
    --nextval('ric.t_ili2db_seq'::regclass),
    cp.id::bigint,
    (SELECT rp.t_id FROM ric.ric_predio rp WHERE rp.numero_predial = cp.numero_predial_unidad LIMIT 1),
    (SELECT rp.t_id FROM ric.ric_predio rp WHERE rp.numero_predial = cp.numero_predial_matriz LIMIT 1),
    COALESCE(cp.coeficiente::numeric(11,10), 'NULL'::numeric(11,10))
FROM tmp_prediocopropiedad cp
WHERE cp.numero_predial_matriz IS NOT NULL
  AND cp.numero_predial_unidad IS NOT NULL
  AND EXISTS (SELECT 1 FROM ric.ric_predio rp WHERE rp.numero_predial = cp.numero_predial_matriz)
  AND EXISTS (SELECT 1 FROM ric.ric_predio rp WHERE rp.numero_predial = cp.numero_predial_unidad);
