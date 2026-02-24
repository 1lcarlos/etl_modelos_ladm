-- INSERT para tabla col_puntoccl
-- Relaciona puntos con linderos
-- Fecha: 2025-12-18
--
-- Dependencias:
--   - Requiere que existan registros en ric_nu_punto y ric_nu_caraslindero
--   - Usa datos de tmp_col_puntoccl (query col_puntoccl.sql)

INSERT INTO ric.col_puntoccl (
    t_id,
    punto,
    ccl
)
SELECT
    nextval('ric.t_ili2db_seq'::regclass),

    -- punto (NOT NULL): Buscar punto en ric_nu_punto
    -- Puede ser puntolindero, puntocontrol o puntolevantamiento
    COALESCE(
        -- Primero buscar por puntolindero
        (SELECT rp.t_id
         FROM ric.ric_nu_punto rp
         WHERE rp.local_id = p.punto_gc_puntolindero::varchar
         LIMIT 1),
        -- Luego por puntocontrol
        (SELECT rp.t_id
         FROM ric.ric_nu_punto rp
         WHERE rp.local_id = p.punto_gc_puntocontrol::varchar
         LIMIT 1),
        -- Finalmente por puntolevantamiento
        (SELECT rp.t_id
         FROM ric.ric_nu_punto rp
         WHERE rp.local_id = p.punto_gc_puntolevantamiento::varchar
         LIMIT 1)
    ),

    -- ccl (NOT NULL): Buscar lindero en ric_nu_caraslindero
    (SELECT rl.t_id
     FROM ric.ric_nu_caraslindero rl
     WHERE rl.local_id = p.ccl::varchar
     LIMIT 1)

FROM tmp_col_puntoccl p
WHERE p.ccl IS NOT NULL
  AND (p.punto_gc_puntolindero IS NOT NULL
       OR p.punto_gc_puntocontrol IS NOT NULL
       OR p.punto_gc_puntolevantamiento IS NOT NULL)
  AND EXISTS (
      SELECT 1 FROM ric.ric_nu_caraslindero rl
      WHERE rl.local_id = p.ccl::varchar
  );
