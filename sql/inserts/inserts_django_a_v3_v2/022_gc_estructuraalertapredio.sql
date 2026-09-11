-- =============================================================================
-- Destino: gc_estructuraalertapredio        Origen: tmp_alerta_predio (extalertas)
-- Reglas:
--   * descripcion_alerta = estado + tipo de documento + numero + fecha del documento.
--   * fecha_apertura_alerta = fecha_creacion; fecha_cierre_alerta no existe en Django.
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'gc_estructuraalertapredio', a.id, 'gc_predio_alerta_predio', a.gc_predio_alerta,
       'RECHAZADO', 'Predio no migrado'
FROM tmp_alerta_predio AS a
LEFT JOIN {schema}.gc_predio AS p ON p.local_id = a.gc_predio_alerta AND p.espacio_de_nombres = 'DJANGO.GC_PREDIO'
WHERE NOT a.es_centinela AND p.t_id IS NULL;

INSERT INTO {schema}.gc_estructuraalertapredio (
    t_basket,
    t_seq,
    descripcion_alerta,
    entidad_emisora_alerta,
    fecha_apertura_alerta,
    fecha_cierre_alerta,
    gc_predio_alerta_predio
)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    row_number() OVER (PARTITION BY p.t_id ORDER BY a.seq::bigint NULLS LAST, a.id::bigint) - 1,
    left(etl_control.texto(concat_ws(' | ',
        'Estado: ' || a.estado,
        'Tipo documento: ' || a.tipo_documento_fuente,
        'Numero: ' || a.numero_fuente,
        'Fecha documento: ' || a.fecha_documento_fuente)), 255),
    etl_control.texto(a.ente_emisor),
    a.fecha_creacion::date,
    NULL,
    p.t_id
FROM tmp_alerta_predio AS a
JOIN {schema}.gc_predio AS p ON p.local_id = a.gc_predio_alerta AND p.espacio_de_nombres = 'DJANGO.GC_PREDIO'
WHERE NOT a.es_centinela;
