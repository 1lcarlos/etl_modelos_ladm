-- =============================================================================
-- Destino: gc_estructuraavaluo (historico completo)   Origen: tmp_avaluo
-- Reglas:
--   * Se migran todas las vigencias. Se excluyen los registros marcados BORRAR en Django.
--   * fecha_avaluo = vigencia.
--   * autoestimacion = false: Django no registra autoestimaciones.
--   * t_seq: 0 = vigencia mas reciente (misma convencion del seq de Django).
--   * Mismo predio con dos avaluos de la misma vigencia: se migran ambos + ADVERTENCIA.
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'gc_estructuraavaluo', a.id, x.campo, x.valor, x.accion, x.motivo
FROM tmp_avaluo AS a
LEFT JOIN {schema}.gc_predio AS p ON p.local_id = a.gc_predio_avaluo AND p.espacio_de_nombres = 'DJANGO.GC_PREDIO'
CROSS JOIN LATERAL (VALUES
    ('descripcion', a.descripcion, 'RECHAZADO', 'Avaluo marcado BORRAR en origen',
     upper(btrim(a.descripcion)) = 'BORRAR'),
    ('gc_predio_avaluo', a.gc_predio_avaluo, 'RECHAZADO', 'Predio no migrado',
     p.t_id IS NULL),
    ('vigencia', a.vigencia, 'ADVERTENCIA', 'El predio tiene mas de un avaluo con esta vigencia',
     a.vigencia_duplicada::boolean)
) AS x(campo, valor, accion, motivo, aplica)
WHERE NOT a.es_centinela AND x.aplica;

INSERT INTO {schema}.gc_estructuraavaluo (
    t_basket,
    t_seq,
    fecha_avaluo,
    avaluo_catastral,
    autoestimacion,
    vigencia,
    por_decreto,
    descripcion,
    gc_predio_avaluo
)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    row_number() OVER (PARTITION BY p.t_id ORDER BY a.vigencia::date DESC NULLS LAST, a.id::bigint) - 1,
    a.vigencia::date,
    a.avaluo_catastral::numeric,
    false,
    a.vigencia::date,
    a.por_decreto::boolean,
    etl_control.texto(a.descripcion),
    p.t_id
FROM tmp_avaluo AS a
JOIN {schema}.gc_predio AS p ON p.local_id = a.gc_predio_avaluo AND p.espacio_de_nombres = 'DJANGO.GC_PREDIO'
WHERE NOT a.es_centinela
  AND upper(btrim(COALESCE(a.descripcion, ''))) <> 'BORRAR';
