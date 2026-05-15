-- Inserción de col_rrrfuente en modelo Django desde interno_v3
-- Dirección: interno_v3 → modelo interno Django
--
-- En v3 el FK es rrr_gc_derechocatastral, en Django es rrr_gc_derecho
-- Los FK se resuelven mediante local_id de las tablas recién insertadas
-- rrr_gc_restriccion: se pasa NULL (gc_restriccion no migrada en este flujo)
-- El id se genera automáticamente por la secuencia col_rrrfuente_id_seq de Django

INSERT INTO {schema}.col_rrrfuente (
    fuente_administrativa,
    rrr_gc_derecho,
    rrr_gc_restriccion
)
SELECT
    fa.id AS fuente_administrativa,
    d.id AS rrr_gc_derecho,
    NULL AS rrr_gc_restriccion
FROM tmp_col_rrrfuente tmp
JOIN {schema}.gc_fuenteadministrativa fa ON fa.local_id = tmp.fuente_administrativa
LEFT JOIN {schema}.gc_derecho d ON d.local_id = tmp.rrr_gc_derecho;
