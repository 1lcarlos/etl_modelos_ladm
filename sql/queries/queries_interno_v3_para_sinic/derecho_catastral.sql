-- Consulta para extraer datos de gc_derechocatastral del modelo interno_v3
-- Dirección: interno_v3 → modelo interno Django
-- En v3 la tabla es gc_derechocatastral, en Django es gc_derecho
-- Se extraen los local_id de predio, interesado y agrupación para resolver FK en el insert

SELECT
    COALESCE(d.espacio_de_nombres, 'GC_DERECHO') AS espacio_de_nombres,
    COALESCE(d.local_id, d.t_id::varchar) AS local_id,
    COALESCE(d.comienzo_vida_util_version::timestamp with time zone, now()) as comienzo_vida_util_version,
    d.fin_vida_util_version::timestamp with time zone as fin_vida_util_version,
    d.descripcion,
    d.t_id as id_derecho,
    p.t_id as id_predio,
    ai.t_id as id_agrupacioninteresados,
    i.t_id as id_interesado,
    dt.ilicode as tipo_derecho
FROM {schema}.gc_derechocatastral d
JOIN {schema}.gc_derechocatastraltipo dt ON dt.t_id = d.tipo
 JOIN {schema}.gc_predio p ON p.t_id = d.unidad
LEFT JOIN {schema}.gc_agrupacioninteresados ai ON ai.t_id = d.interesado_gc_agrupacioninteresados
LEFT JOIN {schema}.gc_interesado i ON i.t_id = d.interesado_gc_interesado
JOIN {schema}.gc_predio_tramitecatastral ptc ON p.t_id = ptc.cr_predio;