-- INSERT para tabla col_unidadfuente
-- Migra relaciones Unidad-Fuente desde la tabla temporal a SINIC
-- Fecha: 2026-02-02
--
-- Dependencias:
--   - Requiere que sinic_predio y cr_fuenteadministrativa ya esten migrados
--   - Usa datos de tmp_sinic_col_unidadfuente (query col_unidadfuente.sql)

INSERT INTO {schema}.col_unidadfuente (
    t_id,
    --t_basket,
    fuente_administrativa,
    unidad
)
SELECT
    uf.id::bigint,
    --(SELECT t_id FROM {schema}.t_ili2db_basket LIMIT 1),

    -- fuente_administrativa: referencia a la fuente administrativa (NOT NULL)
    CASE
        WHEN uf.fuente_administrativa_id IS NOT NULL
             AND EXISTS (SELECT 1 FROM {schema}.cr_fuenteadministrativa fa WHERE fa.t_id = uf.fuente_administrativa_id)
        THEN uf.fuente_administrativa_id
        ELSE NULL
    END,

    -- unidad: referencia al predio (NOT NULL)
    CASE
        WHEN uf.predio_id IS NOT NULL
             AND EXISTS (SELECT 1 FROM {schema}.sinic_predio p WHERE p.t_id = uf.predio_id)
        THEN uf.predio_id
        ELSE NULL
    END

FROM tmp_col_unidadfuente uf
WHERE uf.fuente_administrativa_id IS NOT NULL
  AND uf.predio_id IS NOT NULL;
