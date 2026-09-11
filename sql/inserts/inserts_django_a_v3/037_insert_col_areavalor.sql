INSERT INTO {schema}.col_areavalor (
    t_id,
    tipo,
    area,
    gc_terreno_area,
    gc_unidadconstruccion_area
)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    (SELECT t_id FROM {schema}.col_areatipo
     WHERE ilicode ILIKE '%' || tmp.tipo_area || '%'
     ORDER BY t_id LIMIT 1),
    tmp.area::numeric,
    (SELECT t_id FROM {schema}.gc_terreno
     WHERE local_id::numeric = tmp.terreno_local_id::numeric
     LIMIT 1),
    (SELECT t_id FROM {schema}.gc_unidadconstruccion
     WHERE local_id::numeric = tmp.uc_local_id::numeric
     LIMIT 1)
FROM tmp_col_areavalor tmp
WHERE tmp.area IS NOT NULL;
