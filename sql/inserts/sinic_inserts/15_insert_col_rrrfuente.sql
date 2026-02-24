-- INSERT para tabla col_rrrfuente
-- Migra relaciones RRR-Fuente desde la tabla temporal a SINIC
-- Fecha: 2026-02-02
--
-- Dependencias:
--   - Requiere que sinic_derechocatastral y cr_fuenteadministrativa ya esten migrados
--   - Usa datos de tmp_sinic_col_rrrfuente (query col_rrrfuente.sql)

INSERT INTO {schema}.col_rrrfuente (
    t_id,
    --t_basket,
    fuente_administrativa,
    rrr
)
SELECT
    rf.id::bigint,
    --(SELECT t_id FROM {schema}.t_ili2db_basket LIMIT 1),

    -- fuente_espacial: referencia a la fuente administrativa
    CASE
        WHEN rf.fuente_administrativa_id IS NOT NULL
             AND EXISTS (SELECT 1 FROM {schema}.cr_fuenteadministrativa fa WHERE fa.t_id = rf.fuente_administrativa_id)
        THEN rf.fuente_administrativa_id
        ELSE NULL
    END,

    -- rrr_sinic_derechocatastral: referencia al derecho
    CASE
        WHEN rf.derecho_id IS NOT NULL
             AND EXISTS (SELECT 1 FROM {schema}.sinic_derechocatastral d WHERE d.t_id = rf.derecho_id)
        THEN rf.derecho_id
        ELSE NULL
    END

FROM tmp_col_rrrfuente rf
WHERE rf.derecho_id IS NOT NULL;
