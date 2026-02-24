-- INSERT para tabla ric_fuenteespacial
-- Migra fuentes espaciales desde la tabla temporal a la estructura RIC
-- Fecha: 2025-12-18
--
-- Dependencias:
--   - Usa datos de tmp_gc_fuenteespacial (query gc_fuenteespacial.sql)

INSERT INTO ric.ric_fuenteespacial (
    t_id,
    t_ili_tid,
    tipo,
    metadato,
    nombre,
    descripcion,
    estado_disponibilidad,
    tipo_principal,
    fecha_documento_fuente,
    espacio_de_nombres,
    local_id
)
SELECT
    nextval('ric.t_ili2db_seq'::regclass),
    uuid_generate_v4(),

    -- tipo (NOT NULL): Mapeo a col_fuenteespacialtipo
    COALESCE(
        (SELECT t_id FROM ric.col_fuenteespacialtipo
         WHERE ilicode ILIKE '%' || f.tipo_fuente || '%'
         LIMIT 1),
        (SELECT t_id FROM ric.col_fuenteespacialtipo
         WHERE ilicode = 'Documento'
         LIMIT 1)
    ),

    -- metadato
    NULL,

    -- nombre (NOT NULL)
    COALESCE(f.nombre, 'Fuente_' || f.id::varchar),

    -- descripcion (NOT NULL)
    COALESCE(f.descripcion, 'Sin descripcion'),

    -- estado_disponibilidad (NOT NULL): Mapeo a col_estadodisponibilidadtipo
    COALESCE(
        (SELECT t_id FROM ric.col_estadodisponibilidadtipo
         WHERE ilicode ILIKE '%' || f.estado_disponibilidad || '%'
         LIMIT 1),
        (SELECT t_id FROM ric.col_estadodisponibilidadtipo
         WHERE ilicode = 'Disponible'
         LIMIT 1)
    ),

    -- tipo_principal
    NULL,

    -- fecha_documento_fuente
    f.fecha_documento_fuente::date,

    -- espacio_de_nombres (NOT NULL)
    COALESCE(f.espacio_de_nombres, 'RIC_FUENTEESPACIAL'),

    -- local_id (NOT NULL)
    COALESCE(f.local_id, f.id::varchar)

FROM tmp_gc_fuenteespacial f;
