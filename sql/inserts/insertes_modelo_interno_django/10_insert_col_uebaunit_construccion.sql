-- INSERT para tabla col_uebaunit - relacion construccion-predio (Modelo Interno Django)
-- Relaciona gc_construccion con gc_predio en modelo interno Django
-- Fecha: 2026-02-06
--
-- Origen: tmp_cca_col_uebaunit_construccion (query cca_col_uebaunit_construccion.sql)
-- Destino: col_uebaunit (modelo interno Django)
--
-- Dependencias:
--   - Requiere que gc_predio y gc_construccion ya esten migrados
--
-- IMPORTANTE: Este insert debe ejecutarse DESPUES de gc_predio y gc_construccion

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

    -- ue_gc_construccion: FK a la construccion migrada
    (SELECT gc.id FROM {schema}.gc_construccion gc
     WHERE gc.id = cu.cca_construccion_id
     LIMIT 1),

    -- ue_gc_servidumbretransito (NULL para relacion construccion-predio)
    NULL,

    -- ue_gc_terreno (NULL para relacion construccion-predio)
    NULL,

    -- ue_gc_unidadconstruccion (NULL para relacion construccion-predio)
    NULL

FROM tmp_cca_col_uebaunit_construccion cu
WHERE cu.cca_construccion_id IS NOT NULL
  AND cu.cca_predio_id IS NOT NULL
  AND EXISTS (
      SELECT 1 FROM {schema}.gc_construccion gc
      WHERE gc.id = cu.cca_construccion_id
  )
  AND EXISTS (
      SELECT 1 FROM {schema}.gc_predio gp
      WHERE gp.id = cu.cca_predio_id
  );
