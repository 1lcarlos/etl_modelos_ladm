-- Inserción de terrenos en modelo Django desde interno_v3
-- Dirección: interno_v3 → modelo interno Django
-- El id se genera automáticamente por la secuencia gc_terreno_id_seq de Django

INSERT INTO {schema}.gc_terreno (
    espacio_de_nombres,
    local_id,
    comienzo_vida_util_version,
    fin_vida_util_version,
    codigo,
    etiqueta,
    geometria
)
SELECT
    t.espacio_de_nombres,
    t.local_id,
    t.comienzo_vida_util_version,
    t.fin_vida_util_version,
    t.codigo,
    t.etiqueta,
    t.geometria
FROM tmp_terreno t;
