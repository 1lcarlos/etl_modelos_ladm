-- Destino: gc_tramitecatastral
-- Origen : public.tmp_tramite (tramites de todos los municipios, en la BD Django)
-- public.tmp_tramite trae una fila por predio afectado; aqui se agrupa por resolucion,
-- que es la unidad del tramite en v3.
-- Filtro por municipio: posiciones 1-5 del NPN contra el codigo del esquema (cun25123 -> 25123).
-- tipos_distintos > 1 marca resoluciones con tipo de tramite contradictorio en el origen
-- (el insert lo registra como ADVERTENCIA).
WITH tramites AS (
    SELECT
        t.resolution_number            AS numero_resolucion,
        min(t.tramite_tipo)            AS tipo_mutacion,
        count(DISTINCT t.tramite_tipo) AS tipos_distintos,
        min(t.resolution_date)         AS fecha_resolucion,
        min(t.fecha_radicado)::date    AS fecha_radicacion,
        min(t.numero_radicado)         AS numero_radicado,
        count(*)                       AS predios_afectados
    FROM public.tmp_tramite AS t
    WHERE substring(t.numero_predial, 1, 5) = substring('{schema}', 4, 5)
      AND NULLIF(btrim(t.resolution_number), '') IS NOT NULL
    GROUP BY t.resolution_number
)
SELECT false AS es_centinela, d.* FROM tramites AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN tramites AS d ON false
ORDER BY es_centinela DESC;
