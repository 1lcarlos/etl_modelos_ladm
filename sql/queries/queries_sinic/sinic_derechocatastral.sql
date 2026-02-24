-- Query para extraer datos de gc_derecho para migrar a sinic_derechocatastral
-- Origen: {schema}.gc_derecho
-- Destino: sinic2.sinic_derechocatastral
-- Fecha: 2026-02-02

SELECT DISTINCT ON (d.id)
    d.id,
    COALESCE(d.espacio_de_nombres, 'SINIC_DERECHO') as espacio_de_nombres,
    COALESCE(d.local_id, d.id::varchar) as local_id,
    COALESCE(d.comienzo_vida_util_version, NOW()) as comienzo_vida_util_version,
    d.fin_vida_util_version,
    dt.text_code as tipo_derecho,
    d.descripcion,
    d.fraccion_derecho,
    d.fecha_inicio_tenencia,
    d.baunit as predio_id,
    d.interesado_gc_interesado as interesado_id,
    d.interesado_gc_agrupacioninteresados as agrupacion_id
FROM {schema}.gc_derecho d
LEFT JOIN {schema}.gc_derechotipo dt ON d.tipo = dt.id
ORDER BY d.id;
