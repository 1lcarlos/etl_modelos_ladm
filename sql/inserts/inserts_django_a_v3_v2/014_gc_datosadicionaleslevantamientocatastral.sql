-- =============================================================================
-- Destino: gc_datosadicionaleslevantamientocatastral
-- Origen : tmp_datos_adicionales_levantamiento (dlc de Django)
-- Reglas:
--   * Tipo de documento del reconocedor: texto libre traducido con mapeo_dominio;
--     sin dato -> Sin_Informacion (valor de "desconocido" del propio dominio).
--   * Resultado de visita y estrato: texto libre traducido con mapeo_dominio;
--     sin equivalente -> NULL + incidencia (ya no se asume Sin_Visita).
--   * metodo_levantamiento no existe en Django -> NULL.
-- =============================================================================

INSERT INTO "etl_control"."incidencia" (esquema, tabla_destino, id_origen, campo, valor_origen, accion, motivo)
SELECT '{schema}', 'gc_datosadicionaleslevantamientocatastral', d.id, 'gc_predio', d.gc_predio, 'RECHAZADO',
       'Predio no migrado'
FROM tmp_datos_adicionales_levantamiento AS d
LEFT JOIN {schema}.gc_predio AS p ON p.local_id = d.gc_predio AND p.espacio_de_nombres = 'DJANGO.GC_PREDIO'
WHERE NOT d.es_centinela AND p.t_id IS NULL
UNION ALL
SELECT '{schema}', 'gc_datosadicionaleslevantamientocatastral', d.id, x.campo, x.valor, 'NULIFICADO',
       'Valor sin equivalente en el dominio destino (agregar en 001_mapeo_dominios.sql)'
FROM tmp_datos_adicionales_levantamiento AS d
CROSS JOIN LATERAL (VALUES
    ('tipo_documento_reconocedor', d.document_type_recognizer,
     etl_control.texto(d.document_type_recognizer) IS NOT NULL
     AND etl_control.dominio('{schema}', 'col_documentotipo', etl_control.texto(d.document_type_recognizer)) IS NULL),
    ('resultado_visita', d.resultado_visita,
     etl_control.texto(d.resultado_visita) IS NOT NULL
     AND etl_control.dominio('{schema}', 'gc_resultadovisitatipo', etl_control.texto(d.resultado_visita)) IS NULL),
    ('estrato', d.estrato,
     etl_control.texto(d.estrato) IS NOT NULL
     AND etl_control.dominio('{schema}', 'gc_estratotipo', etl_control.texto(d.estrato)) IS NULL)
) AS x(campo, valor, aplica)
WHERE NOT d.es_centinela AND x.aplica;

INSERT INTO {schema}.gc_datosadicionaleslevantamientocatastral (
    t_basket,
    observaciones,
    fecha_visita_predial,
    tipo_documento_reconocedor,
    numero_documento_reconocedor,
    primer_nombre_reconocedor,
    segundo_nombre_reconocedor,
    primer_apellido_reconocedor,
    segundo_apellido_reconocedor,
    resultado_visita,
    otro_cual_resultado_visita,
    suscribe_acta_colindancia,
    despojo_abandono,
    estrato,
    otro_cual_estrato,
    gc_predio
)
SELECT
    (SELECT b.t_id FROM {schema}.t_ili2db_basket AS b WHERE b.topic = 'Modelo_Interno_ACC_V3.Modelo_Interno'),
    etl_control.texto(d.observaciones),
    d.fecha_visita_predial::date,
    etl_control.dominio('{schema}', 'col_documentotipo',
                        COALESCE(etl_control.texto(d.document_type_recognizer), 'Sin_Informacion')),
    etl_control.texto(d.numero_documento_reconocedor),
    etl_control.texto(d.primer_nombre_reconocedor),
    etl_control.texto(d.segundo_nombre_reconocedor),
    etl_control.texto(d.primer_apellido_reconocedor),
    etl_control.texto(d.segundo_apellido_reconocedor),
    etl_control.dominio('{schema}', 'gc_resultadovisitatipo', etl_control.texto(d.resultado_visita)),
    etl_control.texto(d.otro_cual_resultado_visita),
    d.suscribe_acta_colindancia::boolean,
    d.despojo_abandono::boolean,
    etl_control.dominio('{schema}', 'gc_estratotipo', etl_control.texto(d.estrato)),
    etl_control.texto(d.otro_cual_estrato),
    p.t_id
FROM tmp_datos_adicionales_levantamiento AS d
JOIN {schema}.gc_predio AS p ON p.local_id = d.gc_predio AND p.espacio_de_nombres = 'DJANGO.GC_PREDIO'
WHERE NOT d.es_centinela;
