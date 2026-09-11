INSERT INTO {schema}.gc_estructuraprediomatriznpn (
    t_id,
    numero_predial_nacional,
    area_catastral_terreno,
    gc_predio_predio_matriz_npn
)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    tmp.numero_predial_nacional,
    (SELECT t.area
     FROM {schema}.gc_terreno t
     JOIN {schema}.col_uebaunit ub ON ub.ue_gc_terreno = t.t_id
     JOIN {schema}.gc_predio p2    ON p2.t_id = ub.baunit
                                   AND p2.local_id::numeric = tmp.predio_matriz_local_id::numeric
     LIMIT 1),
    pm.t_id
FROM tmp_gc_estructuraprediomatriznpn tmp
JOIN {schema}.gc_predio pm
    ON pm.local_id::numeric = tmp.predio_matriz_local_id::numeric;
