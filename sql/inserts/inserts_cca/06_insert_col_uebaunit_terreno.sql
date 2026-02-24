-- INSERT para tabla col_uebaunit (relacion terreno-predio)
-- Relaciona gc_terreno con gc_predio en modelo GC
-- Fecha: 2026-02-05
--
-- Dependencias:
--   - Usa datos de tmp_cca_col_uebaunit_terreno (query cca_col_uebaunit_terreno.sql)
--   - Requiere que gc_predio y gc_terreno ya esten migrados
--
-- IMPORTANTE: Este insert debe ejecutarse DESPUES de gc_predio y gc_terreno

INSERT INTO {schema}.col_uebaunit (
    t_id,
    t_ili_tid,
    ue_gc_unidadconstruccion,
    ue_gc_terreno,
    baunit
)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    uuid_generate_v4(),

    -- ue_gc_unidadconstruccion (NULL para relacion terreno-predio)
    NULL,

    -- ue_gc_terreno: Buscar el t_id del terreno migrado usando local_id
    (SELECT gt.t_id FROM {schema}.gc_terreno gt
     WHERE gt.local_id = cu.cca_terreno_id::varchar
     LIMIT 1),

    -- baunit: Buscar el t_id del predio migrado usando numero_predial
    (SELECT gp.t_id FROM {schema}.gc_predio gp
     WHERE gp.numero_predial = cu.numero_predial
     LIMIT 1)

FROM tmp_cca_col_uebaunit_terreno cu
WHERE cu.cca_terreno_id IS NOT NULL
  AND cu.numero_predial IS NOT NULL
  AND EXISTS (
      -- Solo insertar si el terreno ya fue migrado
      SELECT 1 FROM {schema}.gc_terreno gt
      WHERE gt.local_id = cu.cca_terreno_id::varchar
  )
  AND EXISTS (
      -- Solo insertar si el predio ya fue migrado
      SELECT 1 FROM {schema}.gc_predio gp
      WHERE gp.numero_predial = cu.numero_predial
  );
