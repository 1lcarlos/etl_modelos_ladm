-- Vincula unidad predial (PH) con su predio matriz
-- Columnas de área no existen en Django → se insertan como NULL
SELECT
    pu.local_id     AS unidad_predial_local_id,
    pm.local_id     AS matriz_local_id,
    cpc.coeficiente
FROM {schema}.gc_prediocopropiedad cpc
JOIN {schema}.gc_predio pu ON pu.id = cpc.unidad_predial
JOIN {schema}.gc_predio pm ON pm.id = cpc.matriz;
