-- Vincula predios con trámites catastrales por numero_predial
-- Filtrado por municipio según el código del esquema (posiciones 4-8, ej: cun25179 → 25179)
SELECT
    p.numero_predial            AS predio_numero_predial,
    t.resolution_number         AS tramite_numero_resolucion
FROM public.tmp_tramite t
JOIN {schema}.gc_predio p ON p.numero_predial = t.numero_predial
WHERE substring(t.numero_predial, 1, 5) = substring('{schema}', 4, 5)
  AND t.resolution_number IS NOT NULL;
