-- INSERT para tabla col_uebaunit (relacion predio-unidadconstruccion)
-- Migra relaciones predio-unidadconstruccion desde la tabla temporal a SINIC
-- Fecha: 2026-02-02
--
-- Dependencias:
--   - Requiere que sinic_predio y cr_unidadconstruccion ya esten migrados
--   - Usa datos de tmp_sinic_col_uebaunit_unidadconstruccion

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

    -- ue_cr_terreno: NULL para relaciones predio-unidadconstruccion
    NULL,

    -- ue_cr_unidadconstruccion: referencia a la unidad de construccion migrada
    ub.unidadconstruccion_id,

    -- baunit: referencia al predio migrado
    ub.predio_id

FROM tmp_col_uebaunit_unidadconstruccion ub
WHERE EXISTS (SELECT 1 FROM {schema}.sinic_predio p WHERE p.t_id = ub.predio_id)
  AND EXISTS (SELECT 1 FROM {schema}.cr_unidadconstruccion uc WHERE uc.t_id = ub.unidadconstruccion_id);
