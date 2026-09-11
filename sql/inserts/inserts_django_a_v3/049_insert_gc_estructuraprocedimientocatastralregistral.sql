INSERT INTO {schema}.gc_estructuraprocedimientocatastralregistral (
    t_id,
    procedimientocatastralregistral,
    gc_predio_procedimiento_catastral_registral
)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    COALESCE(
        (SELECT t_id FROM {schema}.gc_procedimientocatastralregistraltipo
         WHERE ilicode ILIKE '%' || tmp.tipo_procedimiento || '%'
         ORDER BY t_id LIMIT 1),
        (SELECT t_id FROM {schema}.gc_procedimientocatastralregistraltipo ORDER BY t_id LIMIT 1)
    ),
    p.t_id
FROM tmp_gc_estructuraprocedimientocatastralregistral tmp
JOIN {schema}.gc_predio p
    ON p.local_id::numeric = tmp.predio_local_id::numeric;
