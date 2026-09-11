-- Destino: gc_estructuranovedadnumeropredial
-- En v3 la estructura puede colgar del predio (gc_predio_novedad) o de los datos adicionales
-- (gc_dtsdcnlstmntctstral_novedad_numeros_prediales); cada fila debe ir a uno solo.
--   origen_enlace = 'PREDIO' si Django tiene novedad_gc_predio; 'DLC' si solo tiene el dlc.
-- En Django este dominio usa la columna ilicode (no text_code).
WITH datos AS (
    SELECT
        n.id,
        n.numero_predial,
        tn.ilicode AS tipo_novedad,
        CASE
            WHEN n.novedad_gc_predio IS NOT NULL THEN 'PREDIO'
            WHEN dlc.gc_predio IS NOT NULL THEN 'DLC'
        END AS origen_enlace,
        COALESCE(n.novedad_gc_predio, dlc.gc_predio) AS id_predio
    FROM {schema}.gc_estructuranovedadnumeropredial AS n
    LEFT JOIN {schema}.gc_estructuranovedadnumeropredial_tipo_novedad AS tn ON tn.id = n.tipo_novedad
    LEFT JOIN {schema}.dlc_datosadicionaleslevantamientocatastral AS dlc ON dlc.id = n.gc_predio_novedad_numeros_prediales
)
SELECT false AS es_centinela, d.* FROM datos AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN datos AS d ON false
ORDER BY es_centinela DESC;
