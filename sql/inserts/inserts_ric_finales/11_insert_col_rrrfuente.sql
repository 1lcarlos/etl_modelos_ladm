-- INSERT para tabla col_rrrfuente
-- Relaciona derechos con fuentes administrativas
-- Fecha: 2025-12-18
--
-- Dependencias:
--   - Requiere que existan registros en ric_derecho y ric_fuenteadministrativa
--   - Usa datos de tmp_col_rrrfuente (query col_rrrfuente.sql)

INSERT INTO ric.col_rrrfuente (
    t_id,
    fuente_administrativa,
    rrr
)
SELECT
    --nextval('ric.t_ili2db_seq'::regclass),
    r.id::bigint,
    -- fuente_administrativa (NOT NULL): Buscar el t_id de la fuente
    (SELECT rf.t_id
     FROM ric.ric_fuenteadministrativa rf
     WHERE rf.local_id = r.fuente_administrativa::varchar
     LIMIT 1),

    -- rrr (NOT NULL): Buscar el t_id del derecho
    (SELECT rd.t_id
     FROM ric.ric_derecho rd
     WHERE rd.local_id = r.rrr_gc_derecho::varchar
     LIMIT 1)

FROM tmp_col_rrrfuente r
WHERE r.fuente_administrativa IS NOT NULL
  AND r.rrr_gc_derecho IS NOT NULL
  AND EXISTS (
      SELECT 1 FROM ric.ric_fuenteadministrativa rf
      WHERE rf.local_id = r.fuente_administrativa::varchar
  )
  AND EXISTS (
      SELECT 1 FROM ric.ric_derecho rd
      WHERE rd.local_id = r.rrr_gc_derecho::varchar
  );
