-- INSERT para tabla col_masccl
-- Relaciona linderos con unidades espaciales (lado +)
-- Fecha: 2025-12-18
--
-- Dependencias:
--   - Requiere que existan registros en ric_nu_caraslindero, ric_terreno, ric_unidadconstruccion
--   - Usa datos de tmp_col_masccl (query col_masccl.sql)

INSERT INTO ric.col_masccl (
    t_id,
    ccl_mas,
    ue_mas_ric_terreno,
    ue_mas_ric_unidadconstruccion,
    ue_mas_ric_construccion,
    ue_mas_ric_nu_espaciojuridicoredservicios,
    ue_mas_ric_nu_espaciojuridicounidadedificacion
)
SELECT
    nextval('ric.t_ili2db_seq'::regclass),

    -- ccl_mas (NOT NULL): Buscar lindero en ric_nu_caraslindero
    (SELECT rl.t_id
     FROM ric.ric_nu_caraslindero rl
     WHERE rl.local_id = m.id_lindero::varchar
     LIMIT 1),

    -- ue_mas_ric_terreno: Buscar terreno
    CASE
        WHEN m.id_terreno IS NOT NULL THEN
            (SELECT rt.t_id
             FROM ric.ric_terreno rt
             WHERE rt.local_id = m.id_terreno::varchar
             LIMIT 1)
        ELSE NULL
    END,

    -- ue_mas_ric_unidadconstruccion: Buscar unidad construccion
    CASE
        WHEN m.id_unidad_construccion IS NOT NULL THEN
            (SELECT ru.t_id
             FROM ric.ric_unidadconstruccion ru
             WHERE ru.local_id = m.id_unidad_construccion::varchar
             LIMIT 1)
        ELSE NULL
    END,

    -- ue_mas_ric_construccion (NULL)
    NULL,

    -- ue_mas_ric_nu_espaciojuridicoredservicios (NULL)
    NULL,

    -- ue_mas_ric_nu_espaciojuridicounidadedificacion (NULL)
    NULL

FROM tmp_col_masccl m
WHERE m.id_lindero IS NOT NULL
  AND EXISTS (
      SELECT 1 FROM ric.ric_nu_caraslindero rl
      WHERE rl.local_id = m.id_lindero::varchar
  );
