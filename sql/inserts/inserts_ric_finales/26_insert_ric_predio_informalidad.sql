-- INSERT para tabla ric_predio_informalidad
-- Fecha: 2025-12-19

INSERT INTO ric.ric_predio_informalidad (t_id, ric_predio_formal, ric_predio_informal)
SELECT
    --nextval('ric.t_ili2db_seq'::regclass),
    pi.id::bigint,
    (SELECT rp.t_id FROM ric.ric_predio rp WHERE rp.numero_predial = pi.numero_predial_formal LIMIT 1),
    (SELECT rp.t_id FROM ric.ric_predio rp WHERE rp.numero_predial = pi.numero_predial_informal LIMIT 1)
FROM tmp_predio_informalidad pi
WHERE pi.numero_predial_formal IS NOT NULL
  AND pi.numero_predial_informal IS NOT NULL
  AND EXISTS (SELECT 1 FROM ric.ric_predio rp WHERE rp.numero_predial = pi.numero_predial_formal)
  AND EXISTS (SELECT 1 FROM ric.ric_predio rp WHERE rp.numero_predial = pi.numero_predial_informal);
