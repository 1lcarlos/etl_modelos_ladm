-- INSERT para tabla cr_predio_informalidad
-- Migra relaciones de informalidad desde la tabla temporal a SINIC
-- Fecha: 2026-02-02
--
-- Dependencias:
--   - Requiere que sinic_predio ya este migrado
--   - Usa datos de tmp_sinic_predio_informalidad (query cr_predio_informalidad.sql)

INSERT INTO {schema}.cr_predio_informalidad (
    t_id,
    --t_basket,
    predio_formal,
    predio_informal
)
SELECT
    pi.id::bigint,
    --(SELECT t_id FROM {schema}.t_ili2db_basket LIMIT 1),

    -- predio_formal: referencia al predio formal
    CASE
        WHEN pi.predio_formal_id IS NOT NULL
             AND EXISTS (SELECT 1 FROM {schema}.sinic_predio p WHERE p.t_id = pi.predio_formal_id)
        THEN pi.predio_formal_id
        ELSE NULL
    END,

    -- predio_informal: referencia al predio informal
    CASE
        WHEN pi.predio_informal_id IS NOT NULL
             AND EXISTS (SELECT 1 FROM {schema}.sinic_predio p WHERE p.t_id = pi.predio_informal_id)
        THEN pi.predio_informal_id
        ELSE NULL
    END

FROM tmp_cr_predio_informalidad pi;
