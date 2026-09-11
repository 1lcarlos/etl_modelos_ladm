-- Destino: gc_fuenteespacial
-- ente_emisor no existe en v3: el insert lo reporta como NO_MIGRADO.
WITH datos AS (
    SELECT
        fe.id,
        fe.nombre,
        fe.descripcion,
        fe.fecha_documento_fuente,
        fe.ente_emisor,
        fet.text_code AS tipo,
        edt.text_code AS estado_disponibilidad
    FROM {schema}.gc_fuenteespacial AS fe
    LEFT JOIN {schema}.gc_fuenteespacialtipo AS fet ON fet.id = fe.tipo
    LEFT JOIN {schema}.col_estadodisponibilidadtipo AS edt ON edt.id = fe.estado_disponibilidad
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
