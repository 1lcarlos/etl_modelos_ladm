-- =============================================================================
-- Destino: gc_estructuraprocedimientocatastralregistral (estructura de gc_predio)
-- Origen : tmp_procedimiento_catastral_registral (texto del dlc de Django)
-- El procedimiento es obligatorio y es la unica informacion de la fila:
-- si no tiene equivalente en el dominio, la fila se rechaza.
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'gc_estructuraprocedimientocatastralregistral', d.id, x.campo, x.valor, 'RECHAZADO', x.motivo
FROM tmp_procedimiento_catastral_registral AS d
LEFT JOIN {schema}.gc_predio AS p ON p.local_id = d.gc_predio AND p.espacio_de_nombres = 'DJANGO.GC_PREDIO'
CROSS JOIN LATERAL (VALUES
    ('gc_predio', d.gc_predio, 'Predio no migrado', p.t_id IS NULL),
    ('procedimientocatastralregistral', d.procedimiento_catastral_registral,
     'Valor sin equivalente en el dominio destino (agregar en 001_mapeo_dominios.sql)',
     etl_control.dominio('{schema}', 'gc_procedimientocatastralregistraltipo',
                         etl_control.texto(d.procedimiento_catastral_registral)) IS NULL)
) AS x(campo, valor, motivo, aplica)
WHERE NOT d.es_centinela AND x.aplica;

INSERT INTO {schema}.gc_estructuraprocedimientocatastralregistral (
    t_basket,
    t_seq,
    procedimientocatastralregistral,
    gc_predio_procedimiento_catastral_registral
)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    row_number() OVER (PARTITION BY v.id_predio_destino ORDER BY v.id::bigint) - 1,
    v.procedimiento,
    v.id_predio_destino
FROM (
    SELECT d.id,
           p.t_id AS id_predio_destino,
           etl_control.dominio('{schema}', 'gc_procedimientocatastralregistraltipo',
                               etl_control.texto(d.procedimiento_catastral_registral)) AS procedimiento
    FROM tmp_procedimiento_catastral_registral AS d
    JOIN {schema}.gc_predio AS p ON p.local_id = d.gc_predio AND p.espacio_de_nombres = 'DJANGO.GC_PREDIO'
    WHERE NOT d.es_centinela
) AS v
WHERE v.procedimiento IS NOT NULL;
