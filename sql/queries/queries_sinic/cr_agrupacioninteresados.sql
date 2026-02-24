-- Query para extraer datos de gc_agrupacioninteresados para migrar a cr_agrupacioninteresados
-- Origen: {schema}.gc_agrupacioninteresados
-- Destino: sinic2.cr_agrupacioninteresados
-- Fecha: 2026-02-02

SELECT DISTINCT ON (ai.id)
    ai.id,
    COALESCE(ai.espacio_de_nombres, 'CR_AGRUPACION') as espacio_de_nombres,
    COALESCE(ai.local_id, ai.id::varchar) as local_id,
    COALESCE(ai.comienzo_vida_util_version, NOW()) as comienzo_vida_util_version,
    ai.fin_vida_util_version,
    git.text_code as tipo_grupo,
    ai.nombre,
    NULL::varchar as tipo_interesado,
    NULL::varchar as tipo_documento,
    NULL::varchar as numero_documento
FROM {schema}.gc_agrupacioninteresados ai
LEFT JOIN {schema}.col_grupointeresadotipo git ON ai.tipo = git.id
ORDER BY ai.id;
