-- =============================================================================
-- Destino: gc_fuenteadministrativa         Origen: tmp_fuente_administrativa
-- Reglas:
--   * local_id = id de Django; espacio_de_nombres = 'DJANGO.GC_FUENTEADMINISTRATIVA'.
--   * nombre = numero_fuente (regla del flujo original); si es solo ceros -> NULL.
--   * estado_disponibilidad sin dato -> Desconocido (valor de "desconocido" del dominio).
--   * tipo sin dato -> NULL (antes se forzaba Sin_Documento).
--   * valor_transaccion no tiene columna en la fuente de v3.
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'gc_fuenteadministrativa', f.id, x.campo, x.valor, x.accion, x.motivo
FROM tmp_fuente_administrativa AS f
CROSS JOIN LATERAL (VALUES
    ('nombre', f.numero_fuente, 'NULIFICADO', 'Numero de fuente compuesto solo por ceros',
     btrim(f.numero_fuente) ~ '^0+$'),
    ('tipo', f.tipo, 'NULIFICADO', 'Valor sin equivalente en el dominio destino',
     f.tipo IS NOT NULL AND etl_control.dominio('{schema}', 'col_fuenteadministrativatipo', f.tipo) IS NULL),
    ('estado_disponibilidad', f.estado_disponibilidad, 'NULIFICADO', 'Valor sin equivalente en el dominio destino',
     f.estado_disponibilidad IS NOT NULL
     AND etl_control.dominio('{schema}', 'col_estadodisponibilidadtipo', f.estado_disponibilidad) IS NULL),
    ('valor_transaccion', f.valor_transaccion, 'NO_MIGRADO', 'La fuente administrativa de v3 no tiene valor de transaccion',
     f.valor_transaccion IS NOT NULL)
) AS x(campo, valor, accion, motivo, aplica)
WHERE NOT f.es_centinela AND x.aplica
UNION ALL
SELECT '{schema}', 'gc_fuenteadministrativa', NULL, 'estado_disponibilidad', count(*)::text || ' fuentes', 'DERIVADO',
       'Sin estado de disponibilidad en origen: Desconocido'
FROM tmp_fuente_administrativa AS f
WHERE NOT f.es_centinela AND f.estado_disponibilidad IS NULL
HAVING count(*) > 0;

INSERT INTO {schema}.gc_fuenteadministrativa (
    t_basket,
    tipo,
    ente_emisor,
    estado_disponibilidad,
    fecha_documento_fuente,
    nombre,
    descripcion,
    espacio_de_nombres,
    local_id
)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    etl_control.dominio('{schema}', 'col_fuenteadministrativatipo', f.tipo),
    etl_control.texto(f.ente_emisor),
    etl_control.dominio('{schema}', 'col_estadodisponibilidadtipo', COALESCE(f.estado_disponibilidad, 'Desconocido')),
    f.fecha_documento_fuente::date,
    CASE WHEN btrim(f.numero_fuente) ~ '^0+$' THEN NULL ELSE etl_control.texto(f.numero_fuente) END,
    etl_control.texto(f.descripcion),
    'DJANGO.GC_FUENTEADMINISTRATIVA',
    f.id
FROM tmp_fuente_administrativa AS f
WHERE NOT f.es_centinela;
