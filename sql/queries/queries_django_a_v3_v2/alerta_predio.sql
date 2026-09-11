-- Destino: gc_estructuraalertapredio
-- Origen : extalertas
WITH datos AS (
    SELECT
        ex.id,
        ex.seq,
        ex.gc_predio_alerta,
        ex.ente_emisor,
        ex.numero_fuente,
        ex.fecha_documento_fuente,
        ex.fecha_creacion,
        ge.text_code AS estado,
        gf.text_code AS tipo_documento_fuente
    FROM {schema}.extalertas AS ex
    LEFT JOIN {schema}.gc_estadotipo AS ge ON ge.id = ex.estado
    LEFT JOIN {schema}.gc_fuenteadministrativatipo AS gf ON gf.id = ex.tipo_documento_fuente
    WHERE ex.gc_predio_alerta IS NOT NULL
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
