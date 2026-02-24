-- INSERT para tabla col_miembros
-- Fecha: 2025-12-18

INSERT INTO ric.col_miembros (
    t_id, interesado_ric_interesado, interesado_ric_agrupacioninteresados,
    agrupacion, participacion
)
SELECT
    nextval('ric.t_ili2db_seq'::regclass),
    (SELECT ri.t_id FROM ric.ric_interesado ri WHERE ri.local_id = m.interesado_gc_interesado::varchar LIMIT 1),
    NULL,
    (SELECT ra.t_id FROM ric.ric_agrupacioninteresados ra WHERE ra.local_id = m.agrupacion::varchar LIMIT 1),
    COALESCE(m.participacion::numeric(11,10), 1.0)
FROM tmp_col_miembros m
WHERE m.interesado_gc_interesado IS NOT NULL AND m.agrupacion IS NOT NULL
  AND EXISTS (SELECT 1 FROM ric.ric_interesado ri WHERE ri.local_id = m.interesado_gc_interesado::varchar)
  AND EXISTS (SELECT 1 FROM ric.ric_agrupacioninteresados ra WHERE ra.local_id = m.agrupacion::varchar);
