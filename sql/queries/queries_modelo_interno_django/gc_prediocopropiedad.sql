-- Query para extraer datos de gc_prediocopropiedad
-- Tabla temporal: tmp_prediocopropiedad
-- Fecha: 2025-12-19

SELECT
    cp.id,
    cp.coeficiente,
    cp.matriz,
    cp.unidad_predial,
    -- Referencias para mapeo
    pm.numero_predial AS numero_predial_matriz,
    pu.numero_predial AS numero_predial_unidad
FROM {schema}.gc_prediocopropiedad cp
JOIN {schema}.gc_predio pm 
  ON cp.matriz = pm.id
  AND cp.matriz IS NOT NULL 
  AND SUBSTRING(pm.numero_predial FROM 22 FOR 9) in ('900000000','800000000')
JOIN {schema}.gc_predio pu 
  ON cp.unidad_predial = pu.id
  AND cp.unidad_predial IS NOT NULL;
