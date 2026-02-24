-- INSERT para tabla cr_predio_copropiedad
-- Migra relaciones de copropiedad desde la tabla temporal a SINIC
-- Fecha: 2026-02-02
--
-- Dependencias:
--   - Requiere que sinic_predio ya este migrado
--   - Usa datos de tmp_sinic_predio_copropiedad (query cr_predio_copropiedad.sql)

INSERT INTO {schema}.cr_predio_copropiedad (
    t_id,
    --t_basket,
    coeficiente,
    matriz,
    unidad_predial
)
SELECT
    pc.id::bigint,
    --(SELECT t_id FROM {schema}.t_ili2db_basket LIMIT 1),

    -- coeficiente
    pc.coeficiente,

    -- matriz: referencia al predio matriz
    CASE
        WHEN pc.matriz_id IS NOT NULL
             AND EXISTS (SELECT 1 FROM {schema}.sinic_predio p WHERE p.t_id = pc.matriz_id)
        THEN pc.matriz_id
        ELSE NULL
    END,

    -- unidad_predial: referencia al predio unidad
    CASE
        WHEN pc.unidad_predial_id IS NOT NULL
             AND EXISTS (SELECT 1 FROM {schema}.sinic_predio p WHERE p.t_id = pc.unidad_predial_id)
        THEN pc.unidad_predial_id
        ELSE NULL
    END

FROM tmp_cr_predio_copropiedad pc;
