-- Nota: area_catastral_terreno_coeficiente, area_privada_* no existen en Django
-- Se insertan como coeficiente * terreno.area donde sea posible; caso contrario 0
INSERT INTO {schema}.gc_predio_copropiedad (
    t_id,
    unidad_predial,
    matriz,
    coeficiente,
    area_catastral_terreno_coeficiente,
    area_privada_terreno,
    area_privada_construida,
    area_privada_libre
)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    pu.t_id,
    pm.t_id,
    tmp.coeficiente,
    COALESCE(
        (SELECT t.area * tmp.coeficiente
         FROM {schema}.gc_terreno t
         JOIN {schema}.col_uebaunit ub ON ub.baunit = pu.t_id
                                       AND ub.ue_gc_terreno = t.t_id
         LIMIT 1),
        0
    ),
    NULL,
    NULL,
    NULL
FROM tmp_gc_predio_copropiedad tmp
JOIN {schema}.gc_predio pu ON pu.local_id::numeric = tmp.unidad_predial_local_id::numeric
JOIN {schema}.gc_predio pm ON pm.local_id::numeric = tmp.matriz_local_id::numeric;
