-- INSERT para tabla col_uebaunit (relacion predio-terreno)
-- Migra relaciones predio-terreno desde la tabla temporal a SINIC
-- Fecha: 2026-02-02
--
-- Dependencias:
--   - Requiere que sinic_predio y cr_terreno ya esten migrados
--   - Usa datos de tmp_sinic_col_uebaunit_terreno

INSERT INTO {schema}.col_uebaunit (
    t_id,
    --t_basket,
    ue_cr_terreno,
    ue_cr_unidadconstruccion,
    baunit
)
SELECT
    ub.id::bigint,
    --(SELECT t_id FROM {schema}.t_ili2db_basket LIMIT 1),

    -- ue_cr_terreno: referencia al terreno migrado
    ub.terreno_id,

    -- ue_cr_unidadconstruccion: NULL para relaciones predio-terreno
    NULL,

    -- baunit: referencia al predio migrado
    ub.predio_id

FROM tmp_col_uebaunit_terreno ub
WHERE EXISTS (SELECT 1 FROM {schema}.sinic_predio p WHERE p.t_id = ub.predio_id)
  AND EXISTS (SELECT 1 FROM {schema}.cr_terreno t WHERE t.t_id = ub.terreno_id);
