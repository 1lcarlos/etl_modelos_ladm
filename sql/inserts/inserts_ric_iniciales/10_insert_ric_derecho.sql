-- INSERT para tabla ric_derecho
-- Fecha: 2025-12-18

INSERT INTO ric.ric_derecho (
    t_id, t_ili_tid, tipo, fraccion_derecho, fecha_inicio_tenencia, descripcion,
    interesado_ric_interesado, interesado_ric_agrupacioninteresados, unidad,
    comienzo_vida_util_version, fin_vida_util_version, espacio_de_nombres, local_id
)
SELECT
    nextval('ric.t_ili2db_seq'::regclass),
    uuid_generate_v4(),
    COALESCE(
        (SELECT t_id FROM ric.ric_derechotipo WHERE ilicode ILIKE '%' || d.tipo_derecho || '%' LIMIT 1),
        (SELECT t_id FROM ric.ric_derechotipo WHERE ilicode = 'Dominio' LIMIT 1)
    ),
    COALESCE(d.fraccion_derecho::numeric(11,10), 1.0),
    d.fecha_inicio_tenencia::date,
    d.descripcion,
    CASE WHEN d.interesado_gc_interesado IS NOT NULL THEN
        (SELECT ri.t_id FROM ric.ric_interesado ri WHERE ri.local_id = d.interesado_gc_interesado::varchar LIMIT 1)
    ELSE NULL END,
    CASE WHEN d.interesado_gc_agrupacioninteresados IS NOT NULL THEN
        (SELECT ra.t_id FROM ric.ric_agrupacioninteresados ra WHERE ra.local_id = d.interesado_gc_agrupacioninteresados::varchar LIMIT 1)
    ELSE NULL END,
    (SELECT rp.t_id FROM ric.ric_predio rp WHERE rp.local_id = d.baunit::varchar LIMIT 1),
    COALESCE(d.comienzo_vida_util_version::timestamp, NOW()),
    d.fin_vida_util_version::timestamp,
    COALESCE(d.espacio_de_nombres, 'RIC_DERECHO'),
    COALESCE(d.local_id, d.id::varchar)
FROM tmp_derecho d
WHERE d.baunit IS NOT NULL
  AND EXISTS (SELECT 1 FROM ric.ric_predio rp WHERE rp.local_id = d.baunit::varchar);
