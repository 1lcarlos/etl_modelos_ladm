INSERT INTO {schema}.col_unidadfuente
(t_id,
fuente_administrativa,
unidad)
SELECT
    id,
    fa.t_id,
    p.t_id
FROM
    tmp_col_unidad_fuente tmpunf
    JOIN {schema}.gc_fuenteadministrativa fa ON fa.local_id::numeric = tmpunf.fuente_administrativa::numeric
    JOIN {schema}.gc_predio p on p.local_id::numeric = tmpunf.id_predio::numeric
ON CONFLICT (t_id) DO NOTHING;