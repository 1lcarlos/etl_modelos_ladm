-- INSERT para tabla col_unidadfuente
-- Relaciona predios (baunit) con fuentes administrativas
-- Fecha: 2025-12-18
--
-- Dependencias:
--   - Requiere que existan registros en ric_predio y ric_fuenteadministrativa
--   - Usa datos de tmp_col_unidad_fuente (query col_unidad_fuente.sql)

INSERT INTO ric.col_unidadfuente (
    t_id,
    fuente_administrativa,
    unidad
)
SELECT
    --nextval('ric.t_ili2db_seq'::regclass),
    u.id::bigint,
    -- fuente_administrativa (NOT NULL): Buscar en ric_fuenteadministrativa
    (SELECT rf.t_id
     FROM ric.ric_fuenteadministrativa rf
     WHERE rf.local_id = u.fuente_administrativa::varchar
     LIMIT 1),

    -- unidad (NOT NULL): Buscar predio en ric_predio
    (SELECT rp.t_id
     FROM ric.ric_predio rp
     WHERE rp.local_id = u.id_predio::varchar
     LIMIT 1)

FROM tmp_col_unidad_fuente u
WHERE u.fuente_administrativa IS NOT NULL
  AND u.id_predio IS NOT NULL
  AND EXISTS (
      SELECT 1 FROM ric.ric_fuenteadministrativa rf
      WHERE rf.local_id = u.fuente_administrativa::varchar
  )
  AND EXISTS (
      SELECT 1 FROM ric.ric_predio rp
      WHERE rp.local_id = u.id_predio::varchar
  );
