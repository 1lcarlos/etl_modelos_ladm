-- NPN de cada unidad PH con referencia al predio matriz
SELECT
    pu.numero_predial   AS numero_predial_nacional,
    pm.local_id         AS predio_matriz_local_id
FROM {schema}.gc_prediocopropiedad cpc
JOIN {schema}.gc_predio pu ON pu.id = cpc.unidad_predial
JOIN {schema}.gc_predio pm ON pm.id = cpc.matriz
WHERE pu.numero_predial IS NOT NULL;
