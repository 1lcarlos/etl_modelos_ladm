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
    cr_predio_formal,
    cr_predio_informal,
    area_terreno_interseccion
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

    -- area_terreno_interseccion
    --,coalesce(0, pi.area_terreno_interseccion) -- Si es NULL, se asume 0
    ,0

FROM tmp_cr_predio_informalidad pi;
