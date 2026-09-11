-- Extrae trámites únicos desde public.tmp_tramite
-- Filtrado por municipio según el código del esquema (posiciones 4-8, ej: cun25179 → 25179)
SELECT DISTINCT ON (t.resolution_number)
    t.resolution_number         AS numero_resolucion,
    t.tramite_tipo              AS tipo_mutacion,
    t.resolution_date           AS fecha_resolucion,
    t.fecha_radicado            AS fecha_radicacion
FROM public.tmp_tramite t
WHERE substring(t.numero_predial, 1, 5) = substring('{schema}', 4, 5)
  AND t.resolution_number IS NOT NULL
ORDER BY t.resolution_number;
