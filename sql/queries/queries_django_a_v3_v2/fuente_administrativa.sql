-- Destino: gc_fuenteadministrativa
WITH datos AS (
    SELECT
        fa.id,
        fa.ente_emisor,
        fa.fecha_documento_fuente,
        fa.numero_fuente,
        fa.descripcion,
        fa.valor_transaccion,
        fat.text_code AS tipo,
        edt.text_code AS estado_disponibilidad
    FROM {schema}.gc_fuenteadministrativa AS fa
    LEFT JOIN {schema}.gc_fuenteadministrativatipo AS fat ON fat.id = fa.tipo
    LEFT JOIN {schema}.col_estadodisponibilidadtipo AS edt ON edt.id = fa.estado_disponibilidad
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
