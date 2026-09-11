-- Extrae la relación fuente_administrativa → interesado/agrupacion responsable
-- Cadena: gc_fuenteadministrativa → col_rrrfuente → gc_derecho → interesado
SELECT DISTINCT ON (fa.id, COALESCE(gi.local_id, ga.id::text))
    fa.id::varchar AS fuente_administrativa_local_id,
    gi.local_id    AS interesado_local_id,
    ga.id::varchar AS agrupacion_local_id
FROM {schema}.gc_fuenteadministrativa fa
JOIN {schema}.col_rrrfuente rf ON rf.fuente_administrativa = fa.id
JOIN {schema}.gc_derecho d ON d.id = rf.rrr_gc_derecho
LEFT JOIN {schema}.gc_interesado gi ON gi.id = d.interesado_gc_interesado
LEFT JOIN {schema}.gc_agrupacioninteresados ga ON ga.id = d.interesado_gc_agrupacioninteresados
WHERE d.interesado_gc_interesado IS NOT NULL
   OR d.interesado_gc_agrupacioninteresados IS NOT NULL;
