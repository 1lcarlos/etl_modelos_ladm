-- Consulta para extraer datos de gc_agrupacioninteresados del modelo interno_v3
-- Dirección: interno_v3 → modelo interno Django

SELECT
    COALESCE(ai.espacio_de_nombres, 'gc_agrupacioninteresados') as espacio_de_nombres,
    ai.local_id,
    COALESCE(ai.comienzo_vida_util_version::timestamp with time zone, now()) as comienzo_vida_util_version,
    ai.fin_vida_util_version::timestamp with time zone as fin_vida_util_version,
    ai.t_id as id_agrupacion,
    ai.nombre,
    CASE
        WHEN git.ilicode IS NULL THEN 'Grupo_Civil'
        ELSE git.ilicode
    END as tipo_agrupacion
FROM {schema}.gc_agrupacioninteresados ai
LEFT JOIN {schema}.col_grupointeresadotipo git ON ai.tipo = git.t_id;
