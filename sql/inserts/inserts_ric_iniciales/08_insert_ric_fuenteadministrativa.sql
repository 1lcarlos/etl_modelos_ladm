-- INSERT para tabla ric_fuenteadministrativa
-- Fecha: 2025-12-18

INSERT INTO ric.ric_fuenteadministrativa (
    t_id, t_ili_tid, tipo, ente_emisor, oficina_origen, ciudad_origen,
    observacion, numero_fuente, estado_disponibilidad, tipo_principal,
    fecha_documento_fuente, espacio_de_nombres, local_id
)
SELECT
    nextval('ric.t_ili2db_seq'::regclass),
    uuid_generate_v4(),
    COALESCE(
        (SELECT t_id FROM ric.col_fuenteadministrativatipo WHERE ilicode ILIKE '%' || f.tipo_fuente_administrativa || '%' LIMIT 1),
        (SELECT t_id FROM ric.col_fuenteadministrativatipo WHERE ilicode = 'Documento_Publico' LIMIT 1)
    ),
    f.ente_emisor,
    CASE WHEN f.ente_emisor ~ '^[0-9]+$' THEN LEAST(f.ente_emisor::integer, 100) ELSE 1 END,
    NULL,
    f.descripcion,
    f.numero_fuente,
    COALESCE(
        (SELECT t_id FROM ric.col_estadodisponibilidadtipo WHERE ilicode ILIKE '%' || f.estado_disponibilidad || '%' LIMIT 1),
        (SELECT t_id FROM ric.col_estadodisponibilidadtipo WHERE ilicode = 'Disponible' LIMIT 1)
    ),
    NULL,
    f.fecha_documento_fuente::date,
    COALESCE(f.espacio_de_nombres, 'RIC_FUENTE'),
    COALESCE(f.local_id, f.id::varchar)
FROM tmp_fuente_administrativa f;
