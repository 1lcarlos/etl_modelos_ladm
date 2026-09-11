-- =============================================================================
-- Destino: gc_fuenteespacial               Origen: tmp_fuente_espacial
-- Reglas:
--   * local_id = id de Django; espacio_de_nombres = 'DJANGO.GC_FUENTEESPACIAL'.
--   * tipo: Acta_Colindacia, Acto_Administrativo y Plano no tienen equivalente -> NULL.
--   * estado_disponibilidad sin dato -> Desconocido.
--   * ente_emisor no existe en la fuente espacial de v3.
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'gc_fuenteespacial', f.id, x.campo, x.valor, x.accion, x.motivo
FROM tmp_fuente_espacial AS f
CROSS JOIN LATERAL (VALUES
    ('tipo', f.tipo, 'NULIFICADO', 'Valor sin equivalente en el dominio destino',
     f.tipo IS NOT NULL AND etl_control.dominio('{schema}', 'col_fuenteespacialtipo', f.tipo) IS NULL),
    ('estado_disponibilidad', f.estado_disponibilidad, 'NULIFICADO', 'Valor sin equivalente en el dominio destino',
     f.estado_disponibilidad IS NOT NULL
     AND etl_control.dominio('{schema}', 'col_estadodisponibilidadtipo', f.estado_disponibilidad) IS NULL),
    ('ente_emisor', f.ente_emisor, 'NO_MIGRADO', 'La fuente espacial de v3 no tiene ente emisor',
     etl_control.texto(f.ente_emisor) IS NOT NULL)
) AS x(campo, valor, accion, motivo, aplica)
WHERE NOT f.es_centinela AND x.aplica;

INSERT INTO {schema}.gc_fuenteespacial (
    t_basket,
    tipo,
    estado_disponibilidad,
    fecha_documento_fuente,
    nombre,
    descripcion,
    espacio_de_nombres,
    local_id
)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    etl_control.dominio('{schema}', 'col_fuenteespacialtipo', f.tipo),
    etl_control.dominio('{schema}', 'col_estadodisponibilidadtipo', COALESCE(f.estado_disponibilidad, 'Desconocido')),
    f.fecha_documento_fuente::date,
    etl_control.texto(f.nombre),
    etl_control.texto(f.descripcion),
    'DJANGO.GC_FUENTEESPACIAL',
    f.id
FROM tmp_fuente_espacial AS f
WHERE NOT f.es_centinela;
