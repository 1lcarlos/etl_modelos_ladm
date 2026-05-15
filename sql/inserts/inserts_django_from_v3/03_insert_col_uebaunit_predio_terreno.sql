-- Inserción de relación predio-terreno en col_uebaunit del modelo Django desde interno_v3
-- Dirección: interno_v3 → modelo interno Django
--
-- El id se genera automáticamente por la secuencia col_uebaunit_id_seq de Django
-- Los FK se resuelven mediante join con local_id de las tablas recién insertadas

INSERT INTO {schema}.col_uebaunit (
    ue_gc_terreno,
    unidad
)
SELECT
    t.id,
    p.id
FROM tmp_col_uebaunit_predio_terreno tmp
JOIN {schema}.gc_terreno t ON tmp.ue_gc_terreno = t.local_id
JOIN {schema}.gc_predio p ON tmp.unidad = p.local_id;
