-- Inserción de col_miembros en modelo Django desde interno_v3
-- Dirección: interno_v3 → modelo interno Django
--
-- Los FK se resuelven mediante join con local_id de las tablas recién insertadas
-- Se usa DISTINCT ON para evitar duplicados en los joins
-- El id se genera automáticamente por la secuencia col_miembros_id_seq de Django

INSERT INTO {schema}.col_miembros (
    participacion,
    agrupacion,
    interesado_gc_interesado
)
SELECT
    tcm.participacion::numeric,
    ai.id AS agrupacion,
    i.id AS interesado_gc_interesado
FROM tmp_col_miembros tcm
JOIN (
    SELECT DISTINCT ON (local_id) id, local_id
    FROM {schema}.gc_agrupacioninteresados
) ai ON ai.local_id = tcm.agrupacion_local_id
JOIN (
    SELECT DISTINCT ON (local_id) id, local_id
    FROM {schema}.gc_interesado
) i ON i.local_id = tcm.interesado_local_id;
