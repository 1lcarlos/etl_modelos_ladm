-- INSERT para tabla ric_derecho
-- Migra derechos desde la tabla temporal a la estructura RIC
-- Fecha: 2025-12-18
--
-- Dependencias:
--   - Requiere que existan registros en ric_predio, ric_interesado, ric_agrupacioninteresados
--   - Usa datos de tmp_derecho (query derecho.sql)

INSERT INTO ric.ric_derecho (
    t_id,
    t_ili_tid,
    tipo,
    fraccion_derecho,
    fecha_inicio_tenencia,
    descripcion,
    interesado_ric_interesado,
    interesado_ric_agrupacioninteresados,
    unidad,
    comienzo_vida_util_version,
    fin_vida_util_version,
    espacio_de_nombres,
    local_id
)
SELECT
    --nextval('ric.t_ili2db_seq'::regclass),
    d.id::bigint,
    uuid_generate_v4(),

    -- tipo (NOT NULL): Mapeo a ric_derechotipo
    COALESCE(
        (SELECT t_id FROM ric.ric_derechotipo
         WHERE ilicode ILIKE '%' || d.tipo_derecho || '%'
         LIMIT 1),
        (SELECT t_id FROM ric.ric_derechotipo
         WHERE ilicode = 'Dominio'
         LIMIT 1)
    ),

    -- fraccion_derecho (0.0 a 1.0)
    COALESCE(d.fraccion_derecho::numeric(11,10), 1.0),

    -- fecha_inicio_tenencia
    d.fecha_inicio_tenencia::date,

    -- descripcion
    d.descripcion,

    -- interesado_ric_interesado: Buscar por local_id
    CASE
        WHEN d.interesado_gc_interesado IS NOT NULL THEN
            (SELECT ri.t_id
             FROM ric.ric_interesado ri
             WHERE ri.local_id = d.interesado_gc_interesado::varchar
             LIMIT 1)
        ELSE NULL
    END,

    -- interesado_ric_agrupacioninteresados: Buscar por local_id
    CASE
        WHEN d.interesado_gc_agrupacioninteresados IS NOT NULL THEN
            (SELECT ra.t_id
             FROM ric.ric_agrupacioninteresados ra
             WHERE ra.local_id = d.interesado_gc_agrupacioninteresados::varchar
             LIMIT 1)
        ELSE NULL
    END,

    -- unidad (predio/baunit): Buscar por local_id
    (SELECT rp.t_id
     FROM ric.ric_predio rp
     WHERE rp.local_id = d.baunit::varchar
     LIMIT 1),

    -- comienzo_vida_util_version (NOT NULL)
    COALESCE(d.comienzo_vida_util_version::timestamp, NOW()),

    -- fin_vida_util_version
    d.fin_vida_util_version::timestamp,

    -- espacio_de_nombres (NOT NULL)
    COALESCE(d.espacio_de_nombres, 'RIC_DERECHO'),

    -- local_id (NOT NULL)
    COALESCE(d.id::varchar, d.local_id)

FROM tmp_derecho d
WHERE d.baunit IS NOT NULL
  AND EXISTS (
      SELECT 1 FROM ric.ric_predio rp
      WHERE rp.local_id = d.baunit::varchar
  );
