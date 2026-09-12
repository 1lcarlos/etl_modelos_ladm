-- Destino: gc_predio_tramitecatastral (relacion predio <-> tramite)
-- Origen : public.tmp_tramite + {schema}.gc_predio (para obtener el id Django del predio)
-- Esta consulta NO limita la migracion de predios: solo lista los predios que tienen
-- tramite asociado. Los predios sin tramite se migran igual en 010_gc_predio.sql.
-- Las filas cuyo NPN no existe en el esquema se conservan con id_predio NULL para que
-- el insert las registre como incidencia RECHAZADO.
WITH relaciones AS (
    SELECT DISTINCT
        t.resolution_number AS numero_resolucion,
        t.numero_predial    AS numero_predial,
        p.id                AS id_predio
    FROM public.tmp_tramite AS t
    LEFT JOIN {schema}.gc_predio AS p ON p.numero_predial = t.numero_predial
    WHERE substring(t.numero_predial, 1, 5) = substring('{schema}', 4, 5)
      AND NULLIF(btrim(t.resolution_number), '') IS NOT NULL
)
SELECT false AS es_centinela, d.* FROM relaciones AS d
UNION ALL
SELECT true, d.* FROM (SELECT 1) AS x LEFT JOIN relaciones AS d ON false
ORDER BY es_centinela DESC;
