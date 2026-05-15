-- Inserción de agrupaciones de interesados en modelo Django desde interno_v3
-- Dirección: interno_v3 → modelo interno Django
--
-- Mapeo de dominios: ilicode (interno_v3) → text_code (Django)
-- El id se genera automáticamente por la secuencia gc_agrupacioninteresados_id_seq de Django

INSERT INTO {schema}.gc_agrupacioninteresados (
    espacio_de_nombres,
    local_id,
    comienzo_vida_util_version,
    fin_vida_util_version,
    nombre,
    tipo
)
SELECT
    ai.espacio_de_nombres,
    COALESCE(ai.local_id, ai.id_agrupacion::varchar),
    ai.comienzo_vida_util_version,
    ai.fin_vida_util_version,
    ai.nombre,

    -- tipo (col_grupointeresadotipo): ilicode → text_code
    COALESCE(
        (SELECT id FROM {schema}.col_grupointeresadotipo WHERE text_code = ai.tipo_agrupacion LIMIT 1),
        (SELECT id FROM {schema}.col_grupointeresadotipo WHERE ai.tipo_agrupacion ILIKE '%' || text_code || '%' LIMIT 1),
        (SELECT id FROM {schema}.col_grupointeresadotipo WHERE text_code = 'Grupo_Civil' LIMIT 1)
    ) AS tipo

FROM tmp_agrupacion_interesados ai;
