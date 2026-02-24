-- INSERT para tabla col_miembros
-- Migra miembros de agrupaciones desde la tabla temporal a SINIC
-- Fecha: 2026-02-02
--
-- Dependencias:
--   - Requiere que cr_interesado y cr_agrupacioninteresados ya esten migrados
--   - Usa datos de tmp_sinic_col_miembros

INSERT INTO {schema}.col_miembros (
    t_id,
    --t_basket,
    interesado_cr_interesado,
    --interesado_cr_agrupacioninteresados,
    agrupacion,
    participacion
)
SELECT
    m.id::bigint,
    --(SELECT t_id FROM {schema}.t_ili2db_basket LIMIT 1),

    -- interesado_cr_interesado: referencia al interesado migrado
    m.interesado_id,

    -- interesado_cr_agrupacioninteresados: referencia a agrupacion como interesado
    --m.agrupacion_interesado_id,
    --m.agrupacion_id,

    -- agrupacion: referencia a la agrupacion (NOT NULL)
    m.agrupacion_id,

    -- participacion
    COALESCE(m.participacion::numeric, 1.0)

FROM tmp_col_miembros m
WHERE m.agrupacion_id IS NOT NULL
  AND EXISTS (SELECT 1 FROM {schema}.cr_agrupacioninteresados ai WHERE ai.t_id = m.agrupacion_id);
