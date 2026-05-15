-- Inserción de derechos en modelo Django (gc_derecho) desde interno_v3 (gc_derechocatastral)
-- Dirección: interno_v3 → modelo interno Django
--
-- Mapeo de dominios: gc_derechocatastraltipo.ilicode (v3) → gc_derechotipo.text_code (Django)
-- fraccion_derecho: no existe en gc_derechocatastral (v3), se pone 1 por defecto
-- FK de interesado, agrupación y predio se resuelven mediante local_id
-- El id se genera automáticamente por la secuencia gc_derecho_id_seq de Django

INSERT INTO {schema}.gc_derecho (
    espacio_de_nombres,
    local_id,
    comienzo_vida_util_version,
    fin_vida_util_version,
    descripcion,
    fraccion_derecho,
    baunit,
    interesado_gc_agrupacioninteresados,
    interesado_gc_interesado,
    tipo
)
SELECT
    d.espacio_de_nombres,
    d.local_id,
    d.comienzo_vida_util_version,
    d.fin_vida_util_version,
    d.descripcion,

    -- fraccion_derecho: no existe en v3, valor por defecto 1
    1 AS fraccion_derecho,

    -- baunit (gc_predio): FK resuelta por local_id
    p.id AS baunit,

    -- interesado_gc_agrupacioninteresados: FK resuelta por local_id
    ai.id AS interesado_gc_agrupacioninteresados,

    -- interesado_gc_interesado: FK resuelta por local_id
    i.id AS interesado_gc_interesado,

    -- tipo (gc_derechotipo): ilicode de gc_derechocatastraltipo → text_code
    COALESCE(
        (SELECT id FROM {schema}.gc_derechotipo WHERE text_code = d.tipo_derecho LIMIT 1),
        (SELECT id FROM {schema}.gc_derechotipo WHERE d.tipo_derecho ILIKE '%' || text_code || '%' LIMIT 1)
    ) AS tipo

FROM tmp_derecho d
LEFT JOIN {schema}.gc_predio p ON p.local_id = d.baunit
LEFT JOIN (
    SELECT DISTINCT ON (local_id) id, local_id
    FROM {schema}.gc_agrupacioninteresados
) ai ON ai.local_id = d.interesado_gc_agrupacioninteresados
LEFT JOIN (
    SELECT DISTINCT ON (local_id) id, local_id
    FROM {schema}.gc_interesado
) i ON i.local_id = d.interesado_gc_interesado;
