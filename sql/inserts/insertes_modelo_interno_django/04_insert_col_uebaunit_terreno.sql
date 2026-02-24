-- INSERT para tabla col_uebaunit - relacion terreno-predio (Modelo Interno Django)
-- Relaciona gc_terreno con gc_predio en modelo interno Django
-- Fecha: 2026-02-06
--
-- Origen: tmp_cca_col_uebaunit_terreno (query cca_col_uebaunit_terreno.sql)
-- Destino: col_uebaunit (modelo interno Django)
--
-- Diferencias clave con modelo INTERLIS:
--   - PK es 'id' auto-generado (no t_id)
--   - No existe t_ili_tid
--   - FK 'unidad' en vez de 'baunit' para la referencia al predio
--   - FKs apuntan a 'id' de las tablas relacionadas (no t_id)
--
-- Dependencias:
--   - Requiere que gc_predio y gc_terreno ya esten migrados
--
-- IMPORTANTE: Este insert debe ejecutarse DESPUES de gc_predio y gc_terreno

INSERT INTO {schema}.col_uebaunit (
    unidad,
    ue_gc_construccion,
    ue_gc_servidumbretransito,
    ue_gc_terreno,
    ue_gc_unidadconstruccion
)
SELECT
    -- unidad: Buscar el id del predio migrado usando numero_predial
    (SELECT gp.id FROM {schema}.gc_predio gp
     WHERE gp.id = cu.cca_predio_id
     LIMIT 1),

    -- ue_gc_construccion (NULL para relacion terreno-predio)
    NULL,

    -- ue_gc_servidumbretransito (NULL para relacion terreno-predio)
    NULL,

    -- ue_gc_terreno: Buscar el id del terreno migrado usando local_id
    (SELECT gt.id FROM {schema}.gc_terreno gt
     WHERE gt.id = cu.cca_terreno_id::numeric
     LIMIT 1),

    -- ue_gc_unidadconstruccion (NULL para relacion terreno-predio)
    NULL

FROM tmp_cca_col_uebaunit_terreno cu
WHERE cu.cca_terreno_id IS NOT NULL
  AND cu.cca_predio_id IS NOT NULL
  AND EXISTS (
      -- Solo insertar si el terreno ya fue migrado
      SELECT 1 FROM {schema}.gc_terreno gt
      WHERE gt.id = cu.cca_terreno_id::numeric
  )
  AND EXISTS (
      -- Solo insertar si el predio ya fue migrado
      SELECT 1 FROM {schema}.gc_predio gp
      WHERE gp.id = cu.cca_predio_id
  );
