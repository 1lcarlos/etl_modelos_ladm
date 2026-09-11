-- Destino: gc_datosadicionaleslevantamientocatastral
-- Los textos libres (tipo documento reconocedor, resultado visita, estrato) se traducen
-- en el insert con etl_control.mapeo_dominio (ver 001_mapeo_dominios.sql).
WITH datos AS (
    SELECT
        d.id,
        d.gc_predio,
        d.observaciones,
        d.fecha_visita_predial,
        d.document_type_recognizer,
        d.numero_documento_reconocedor,
        d.primer_nombre_reconocedor,
        d.segundo_nombre_reconocedor,
        d.primer_apellido_reconocedor,
        d.segundo_apellido_reconocedor,
        d.resultado_visita,
        d.otro_cual_resultado_visita,
        d.suscribe_acta_colindancia,
        d.despojo_abandono,
        d.estrato,
        d.otro_cual_estrato
    FROM {schema}.dlc_datosadicionaleslevantamientocatastral AS d
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
