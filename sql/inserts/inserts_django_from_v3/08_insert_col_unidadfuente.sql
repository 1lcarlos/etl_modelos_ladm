-- Inserción de col_unidadfuente en modelo Django desde interno_v3
-- Dirección: interno_v3 → modelo interno Django
--
-- Los FK se resuelven mediante join con local_id de las tablas recién insertadas
-- El id se genera automáticamente por la secuencia col_unidadfuente_id_seq de Django

INSERT INTO {schema}.col_unidadfuente (
    unidad,
    fuente_administrativa
)
SELECT
    p.id,
    fa.id
FROM tmp_col_unidad_fuente tmp
JOIN {schema}.gc_predio p ON p.local_id = tmp.id_predio
JOIN {schema}.gc_fuenteadministrativa fa ON fa.local_id = tmp.fuente_administrativa;
