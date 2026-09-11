INSERT INTO {schema}.gc_estructuraavaluocaracterizacion (
    t_id,
    fecha_avaluo,
    valor_comercial,
    avaluo_catastral,
    gc_crctrstcdcnstrccion_avaluo_caracterizacion
)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    tmp.fecha_avaluo::date,
    tmp.valor_comercial,
    tmp.avaluo_catastral,
    cuc.t_id
FROM tmp_gc_estructuraavaluocaracterizacion tmp
JOIN {schema}.gc_caracteristicasunidadconstruccion cuc
    ON cuc.local_id::numeric = tmp.caracteristicas_local_id::numeric
WHERE tmp.avaluo_catastral IS NOT NULL;
