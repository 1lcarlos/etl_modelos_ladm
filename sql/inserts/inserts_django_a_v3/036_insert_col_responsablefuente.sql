INSERT INTO {schema}.col_responsablefuente (
    t_id,
    fuente_administrativa,
    interesado_gc_interesado,
    interesado_gc_agrupacioninteresados
)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    fa.t_id,
    gi.t_id,
    ga.t_id
FROM tmp_col_responsablefuente tmp
JOIN {schema}.gc_fuenteadministrativa fa
    ON fa.local_id::numeric = tmp.fuente_administrativa_local_id::numeric
LEFT JOIN {schema}.gc_interesado gi
    ON gi.local_id::numeric = (
        CASE WHEN tmp.interesado_local_id ~ '^[0-9]+$'
             THEN tmp.interesado_local_id::numeric
             ELSE NULL END
    )
LEFT JOIN {schema}.gc_agrupacioninteresados ga
    ON ga.local_id ~ '^[0-9]+$'
    AND ga.local_id::numeric = tmp.agrupacion_local_id::numeric
WHERE fa.t_id IS NOT NULL
  AND (gi.t_id IS NOT NULL OR ga.t_id IS NOT NULL);
