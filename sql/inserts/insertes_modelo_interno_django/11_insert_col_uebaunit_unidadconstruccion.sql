-- INSERT para tabla col_uebaunit - relacion unidadconstruccion-predio (Modelo Interno Django)
-- Relaciona gc_unidadconstruccion con gc_predio en modelo interno Django
-- Fecha: 2026-02-06
--
-- Origen: tmp_cca_col_uebaunit_unidadconstruccion (query cca_col_uebaunit_unidadconstruccion.sql)
-- Destino: col_uebaunit (modelo interno Django)
--
-- Dependencias:
--   - Requiere que gc_predio y gc_unidadconstruccion ya esten migrados
--
-- IMPORTANTE: Este insert debe ejecutarse DESPUES de gc_predio y gc_unidadconstruccion

INSERT INTO {schema}.col_uebaunit (
    unidad,
    ue_gc_construccion,
    ue_gc_servidumbretransito,
    ue_gc_terreno,
    ue_gc_unidadconstruccion
)
SELECT
    -- unidad: FK al predio migrado
    (SELECT gp.id FROM {schema}.gc_predio gp
     WHERE gp.id = cu.cca_predio_id
     LIMIT 1),

    -- ue_gc_construccion (NULL para relacion unidadconstruccion-predio)
    NULL,

    -- ue_gc_servidumbretransito (NULL para relacion unidadconstruccion-predio)
    NULL,

    -- ue_gc_terreno (NULL para relacion unidadconstruccion-predio)
    NULL,

    -- ue_gc_unidadconstruccion: FK a la unidad de construccion migrada
    (SELECT guc.id FROM {schema}.gc_unidadconstruccion guc
     WHERE guc.id = cu.cca_unidadconstruccion_id
     LIMIT 1)

FROM tmp_cca_col_uebaunit_unidadconstruccion cu
WHERE cu.cca_unidadconstruccion_id IS NOT NULL
  AND cu.cca_predio_id IS NOT NULL
  AND EXISTS (
      SELECT 1 FROM {schema}.gc_unidadconstruccion guc
      WHERE guc.id = cu.cca_unidadconstruccion_id
  )
  AND EXISTS (
      SELECT 1 FROM {schema}.gc_predio gp
      WHERE gp.id = cu.cca_predio_id
  );
