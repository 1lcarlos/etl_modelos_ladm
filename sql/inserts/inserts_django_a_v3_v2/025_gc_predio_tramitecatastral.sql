-- =============================================================================
-- Destino: gc_predio_tramitecatastral   Origen: tmp_predio_tramite_catastral
-- Relacion predio <-> tramite: una fila por predio afectado por la resolucion.
-- IMPORTANTE: este archivo no condiciona la migracion de predios. Todos los predios
-- se migran en 010_gc_predio.sql; aqui solo se agrega la relacion cuando el predio y
-- el tramite existen en el destino.
-- Se enlaza con el predio por local_id (id de Django) + espacio_de_nombres, nunca por NPN,
-- y con el tramite por numero_resolucion.
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'gc_predio_tramitecatastral', r.numero_resolucion, x.campo, x.valor, 'RECHAZADO', x.motivo
FROM tmp_predio_tramite_catastral AS r
LEFT JOIN {schema}.gc_predio AS p
       ON p.local_id = r.id_predio AND p.espacio_de_nombres = 'DJANGO.GC_PREDIO'
LEFT JOIN {schema}.gc_tramitecatastral AS t ON t.numero_resolucion = r.numero_resolucion
CROSS JOIN LATERAL (VALUES
    ('cr_predio', r.numero_predial,
     'El NPN del tramite no corresponde a ningun predio del esquema en Django',
     r.id_predio IS NULL),
    ('cr_predio', r.numero_predial, 'Predio no migrado al destino',
     r.id_predio IS NOT NULL AND p.t_id IS NULL),
    ('cr_tramite_catastral', r.numero_resolucion,
     'Tramite no migrado (tipo sin equivalente en gc_mutaciontipo)',
     t.t_id IS NULL)
) AS x(campo, valor, motivo, aplica)
WHERE NOT r.es_centinela AND x.aplica;

INSERT INTO {schema}.gc_predio_tramitecatastral (
    t_basket,
    cr_predio,
    cr_tramite_catastral
)
SELECT DISTINCT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    p.t_id,
    t.t_id
FROM tmp_predio_tramite_catastral AS r
JOIN {schema}.gc_predio AS p
  ON p.local_id = r.id_predio AND p.espacio_de_nombres = 'DJANGO.GC_PREDIO'
JOIN {schema}.gc_tramitecatastral AS t ON t.numero_resolucion = r.numero_resolucion
WHERE NOT r.es_centinela;
