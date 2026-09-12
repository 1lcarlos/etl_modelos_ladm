-- =============================================================================
-- Destino: gc_tramitecatastral      Origen: tmp_tramite_catastral (public.tmp_tramite)
-- Una fila por resolucion. Requiere que la BD origen tenga public.tmp_tramite.
-- Reglas (decisiones 2026-09-11):
--   * clasificacion_mutacion: etl_control.dominio sobre gc_mutaciontipo (mapeo en 001).
--     Es obligatorio en v3: sin equivalente la fila se rechaza.
--   * fecha_inscripcion: Django no la registra; se usa la fecha de la resolucion (DERIVADO).
--   * numero_radicado: v3 no tiene columna para el radicado (NO_MIGRADO).
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'gc_tramitecatastral', t.numero_resolucion, x.campo, x.valor, x.accion, x.motivo
FROM tmp_tramite_catastral AS t
CROSS JOIN LATERAL (VALUES
    ('clasificacion_mutacion', t.tipo_mutacion, 'RECHAZADO',
     'Tipo de tramite sin equivalente en gc_mutaciontipo (agregar en 001_mapeo_dominios.sql)',
     etl_control.dominio('{schema}', 'gc_mutaciontipo', etl_control.texto(t.tipo_mutacion)) IS NULL),
    ('numero_radicado', t.numero_radicado, 'NO_MIGRADO',
     'v3 no tiene columna para el numero de radicado del tramite', true),
    ('fecha_inscripcion', t.fecha_resolucion, 'DERIVADO',
     'Django no registra la fecha de inscripcion: se usa la fecha de la resolucion', true),
    ('tipo_mutacion', t.tipo_mutacion, 'ADVERTENCIA',
     'La resolucion tiene mas de un tipo de tramite en el origen: se toma el primero alfabeticamente',
     t.tipos_distintos::int > 1)
) AS x(campo, valor, accion, motivo, aplica)
WHERE NOT t.es_centinela AND x.aplica;

INSERT INTO {schema}.gc_tramitecatastral (
    t_basket,
    clasificacion_mutacion,
    numero_resolucion,
    fecha_resolucion,
    fecha_radicacion,
    fecha_inscripcion
)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    etl_control.dominio('{schema}', 'gc_mutaciontipo', etl_control.texto(t.tipo_mutacion)),
    t.numero_resolucion,
    t.fecha_resolucion::date,
    t.fecha_radicacion::date,
    t.fecha_resolucion::date
FROM tmp_tramite_catastral AS t
WHERE NOT t.es_centinela
  AND etl_control.dominio('{schema}', 'gc_mutaciontipo', etl_control.texto(t.tipo_mutacion)) IS NOT NULL;
