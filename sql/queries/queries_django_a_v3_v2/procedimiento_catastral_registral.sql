-- Destino: gc_estructuraprocedimientocatastralregistral
-- Origen : dlc_datosadicionaleslevantamientocatastral.procedimiento_catastral_registral (texto)
WITH datos AS (
    SELECT
        d.id,
        d.gc_predio,
        d.procedimiento_catastral_registral
    FROM {schema}.dlc_datosadicionaleslevantamientocatastral AS d
    WHERE NULLIF(btrim(d.procedimiento_catastral_registral), '') IS NOT NULL
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
