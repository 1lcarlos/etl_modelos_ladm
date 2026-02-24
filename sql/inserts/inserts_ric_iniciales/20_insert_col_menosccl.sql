-- INSERT para tabla col_menosccl
-- Relaciona linderos con unidades espaciales (lado -)
-- Fecha: 2025-12-18
--
-- Dependencias:
--   - Requiere que existan registros en ric_nu_caraslindero, ric_terreno, ric_construccion, ric_unidadconstruccion
--   - Usa datos de tmp_col_menosccl (query col_menosccl.sql)

INSERT INTO ric.col_menosccl (
    t_id,
    ccl_menos,
    ue_menos_ric_terreno,
    ue_menos_ric_unidadconstruccion,
    ue_menos_ric_construccion,
    ue_menos_ric_nu_espaciojuridicoredservicios,
    ue_menos_ric_nu_espaciojuridicounidadedificacion
)
SELECT
    nextval('ric.t_ili2db_seq'::regclass),

    -- ccl_menos (NOT NULL): Buscar lindero en ric_nu_caraslindero
    (SELECT rl.t_id
     FROM ric.ric_nu_caraslindero rl
     WHERE rl.local_id = m.ccl_menos::varchar
     LIMIT 1),

    -- ue_menos_ric_terreno: Buscar terreno
    CASE
        WHEN m.ue_menos_gc_terreno IS NOT NULL THEN
            (SELECT rt.t_id
             FROM ric.ric_terreno rt
             WHERE rt.local_id = m.ue_menos_gc_terreno::varchar
             LIMIT 1)
        ELSE NULL
    END,

    -- ue_menos_ric_unidadconstruccion: Buscar unidad construccion
    CASE
        WHEN m.ue_menos_gc_unidadconstruccion IS NOT NULL THEN
            (SELECT ru.t_id
             FROM ric.ric_unidadconstruccion ru
             WHERE ru.local_id = m.ue_menos_gc_unidadconstruccion::varchar
             LIMIT 1)
        ELSE NULL
    END,

    -- ue_menos_ric_construccion: Buscar construccion
    CASE
        WHEN m.ue_menos_gc_construccion IS NOT NULL THEN
            (SELECT rc.t_id
             FROM ric.ric_construccion rc
             WHERE rc.local_id = m.ue_menos_gc_construccion::varchar
             LIMIT 1)
        ELSE NULL
    END,

    -- ue_menos_ric_nu_espaciojuridicoredservicios (NULL)
    NULL,

    -- ue_menos_ric_nu_espaciojuridicounidadedificacion (NULL)
    NULL

FROM tmp_col_menosccl m
WHERE m.ccl_menos IS NOT NULL
  AND EXISTS (
      SELECT 1 FROM ric.ric_nu_caraslindero rl
      WHERE rl.local_id = m.ccl_menos::varchar
  );
