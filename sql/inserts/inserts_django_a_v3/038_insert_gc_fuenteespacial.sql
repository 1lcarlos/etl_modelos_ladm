INSERT INTO {schema}.gc_fuenteespacial (
    t_id,
    t_ili_tid,
    tipo,
    estado_disponibilidad,
    fecha_documento_fuente,
    nombre,
    descripcion,
    espacio_de_nombres,
    local_id
)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    uuid_generate_v4(),
    COALESCE(
        (SELECT t_id FROM {schema}.col_fuenteespacialtipo
         WHERE ilicode ILIKE '%' || tmp.tipo_fuente_espacial || '%'
         ORDER BY t_id LIMIT 1),
        (SELECT t_id FROM {schema}.col_fuenteespacialtipo ORDER BY t_id LIMIT 1)
    ),
    COALESCE(
        (SELECT t_id FROM {schema}.col_estadodisponibilidadtipo
         WHERE ilicode ILIKE '%' || tmp.estado_disponibilidad || '%'
         ORDER BY t_id LIMIT 1),
        (SELECT t_id FROM {schema}.col_estadodisponibilidadtipo ORDER BY t_id LIMIT 1)
    ),
    tmp.fecha_documento_fuente::date,
    tmp.nombre,
    tmp.descripcion,
    tmp.espacio_de_nombres,
    tmp.local_id
FROM tmp_gc_fuenteespacial tmp;
