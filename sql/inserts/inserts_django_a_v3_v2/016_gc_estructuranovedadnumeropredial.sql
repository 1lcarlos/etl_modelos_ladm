-- =============================================================================
-- Destino: gc_estructuranovedadnumeropredial
-- Origen : tmp_novedad_numero_predial
-- Cada fila cuelga de un solo padre:
--   origen_enlace = PREDIO -> gc_predio_novedad
--   origen_enlace = DLC    -> gc_dtsdcnlstmntctstral_novedad_numeros_prediales
--                             (datos adicionales del mismo predio en el destino)
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'gc_estructuranovedadnumeropredial', n.id, x.campo, x.valor, x.accion, x.motivo
FROM tmp_novedad_numero_predial AS n
LEFT JOIN {schema}.gc_predio AS p ON p.local_id = n.id_predio AND p.espacio_de_nombres = 'DJANGO.GC_PREDIO'
LEFT JOIN LATERAL (
    SELECT d.t_id FROM {schema}.gc_datosadicionaleslevantamientocatastral AS d
    WHERE d.gc_predio = p.t_id ORDER BY d.t_id DESC LIMIT 1
) AS dlc ON true
CROSS JOIN LATERAL (VALUES
    ('gc_predio_novedad', n.id_predio, 'RECHAZADO', 'Sin predio o predio no migrado', p.t_id IS NULL),
    ('gc_dtsdcnlstmntctstral_novedad_numeros_prediales', n.id_predio, 'RECHAZADO',
     'La novedad cuelga de datos adicionales que no existen en el destino',
     p.t_id IS NOT NULL AND n.origen_enlace = 'DLC' AND dlc.t_id IS NULL),
    ('tipo_novedad', n.tipo_novedad, 'NULIFICADO', 'Valor sin equivalente en el dominio destino',
     n.tipo_novedad IS NOT NULL
     AND etl_control.dominio('{schema}', 'gc_estructuranovedadnumeropredial_tipo_novedad', n.tipo_novedad) IS NULL)
) AS x(campo, valor, accion, motivo, aplica)
WHERE NOT n.es_centinela AND x.aplica;

INSERT INTO {schema}.gc_estructuranovedadnumeropredial (
    t_basket,
    t_seq,
    numero_predial,
    tipo_novedad,
    gc_dtsdcnlstmntctstral_novedad_numeros_prediales,
    gc_predio_novedad
)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    row_number() OVER (PARTITION BY n.origen_enlace, p.t_id ORDER BY n.id::bigint) - 1,
    n.numero_predial,
    etl_control.dominio('{schema}', 'gc_estructuranovedadnumeropredial_tipo_novedad', n.tipo_novedad),
    CASE WHEN n.origen_enlace = 'DLC' THEN dlc.t_id END,
    CASE WHEN n.origen_enlace = 'PREDIO' THEN p.t_id END
FROM tmp_novedad_numero_predial AS n
JOIN {schema}.gc_predio AS p ON p.local_id = n.id_predio AND p.espacio_de_nombres = 'DJANGO.GC_PREDIO'
LEFT JOIN LATERAL (
    SELECT d.t_id FROM {schema}.gc_datosadicionaleslevantamientocatastral AS d
    WHERE d.gc_predio = p.t_id ORDER BY d.t_id DESC LIMIT 1
) AS dlc ON true
WHERE NOT n.es_centinela
  AND (n.origen_enlace = 'PREDIO' OR dlc.t_id IS NOT NULL);
