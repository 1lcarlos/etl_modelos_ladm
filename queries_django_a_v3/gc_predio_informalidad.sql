-- Vincula predio formal con predio informal
-- Columnas de área de intersección no existen en Django → se insertan como 0
SELECT
    pf.local_id  AS predio_formal_local_id,
    pi.local_id  AS predio_informal_local_id,
    inf.id
FROM {schema}.gc_predio_informalidad inf
JOIN {schema}.gc_predio pf ON pf.id = inf.predio_formal
JOIN {schema}.gc_predio pi ON pi.id = inf.predio_informal;
