-- INSERT para tabla col_miembros
-- Relaciona interesados con agrupaciones de interesados
-- Fecha: 2025-12-18
--
-- Dependencias:
--   - Requiere que existan registros en ric_interesado y ric_agrupacioninteresados
--   - Usa datos de tmp_col_miembros (query col_miembros.sql)

INSERT INTO ric.col_miembros (
    t_id,
    interesado_ric_interesado,    
    agrupacion,
    participacion
)
SELECT
    --nextval('ric.t_ili2db_seq'::regclass),
    m.id::bigint,
    -- interesado_ric_interesado: Buscar el t_id del interesado
    (SELECT ri.t_id
     FROM ric.ric_interesado ri
     WHERE ri.local_id = m.interesado_gc_interesado::varchar
     LIMIT 1),   

    -- agrupacion (NOT NULL): Buscar el t_id de la agrupacion
    (SELECT ra.t_id
     FROM ric.ric_agrupacioninteresados ra
     WHERE ra.local_id = m.agrupacion::varchar
     LIMIT 1),

    -- participacion
    COALESCE(m.participacion::numeric(11,10), 1.0)

FROM tmp_col_miembros m
WHERE m.interesado_gc_interesado IS NOT NULL
  AND m.agrupacion IS NOT NULL
  AND EXISTS (
      SELECT 1 FROM ric.ric_interesado ri
      WHERE ri.local_id = m.interesado_gc_interesado::varchar
  )
  AND EXISTS (
      SELECT 1 FROM ric.ric_agrupacioninteresados ra
      WHERE ra.local_id = m.agrupacion::varchar
  );
