-- INSERT para tabla col_uebaunit (relacion unidad construccion - predio)
-- Relaciona unidades de construccion RIC con predios RIC
-- Fecha: 2025-12-18
--
-- Dependencias:
--   - Requiere que existan registros en ric_unidadconstruccion y ric_predio
--   - Usa datos de tmp_col_uebaunit_predio_unidadconstruccion (query col_uebaunit_predio_unidadconstruccion.sql)

INSERT INTO ric.col_uebaunit (
    t_id,
    ue_ric_terreno,
    ue_ric_unidadconstruccion,
    ue_ric_construccion,
    ue_ric_nu_espaciojuridicoredservicios,
    ue_ric_nu_espaciojuridicounidadedificacion,
    baunit
)
SELECT
    nextval('ric.t_ili2db_seq'::regclass),
    --cu.id::bigint,
    -- ue_ric_terreno (NULL para unidades de construccion)
    NULL,

    -- ue_ric_unidadconstruccion: Buscar el t_id de la unidad construccion
    (SELECT ru.t_id
     FROM ric.ric_unidadconstruccion ru
     WHERE ru.local_id::integer = cu.ue_gc_unidadconstruccion::integer
     LIMIT 1),

    -- ue_ric_construccion (NULL)
    NULL,

    -- ue_ric_nu_espaciojuridicoredservicios (NULL)
    NULL,

    -- ue_ric_nu_espaciojuridicounidadedificacion (NULL)
    NULL,

    -- baunit: Buscar el t_id del predio en ric_predio
    (SELECT rp.t_id
     FROM ric.ric_predio rp
     WHERE rp.local_id::integer = cu.baunit::integer
     LIMIT 1)

FROM tmp_col_uebaunit_predio_unidadconstruccion cu
WHERE cu.ue_gc_unidadconstruccion IS NOT NULL
  AND cu.baunit IS NOT NULL
  AND EXISTS (
      SELECT 1 FROM ric.ric_unidadconstruccion ru
      WHERE ru.local_id::integer = cu.ue_gc_unidadconstruccion::integer
  )
  AND EXISTS (
      SELECT 1 FROM ric.ric_predio rp
      WHERE rp.local_id::integer = cu.baunit::integer
  );
