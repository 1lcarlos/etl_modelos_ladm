-- Query para extraer datos de gc_predio_informalidad
-- Tabla temporal: tmp_predio_informalidad
-- Fecha: 2025-12-19

SELECT
    pi.id,
    pi.predio_formal,
    pi.predio_informal,
    pf.numero_predial   AS numero_predial_formal,
    pinf.numero_predial AS numero_predial_informal
FROM {schema}.gc_predio_informalidad pi
JOIN {schema}.gc_predio pf 
    ON pi.predio_formal = pf.id
   AND length(pf.numero_predial) >= 22
   AND substring(pf.numero_predial FROM 22 FOR 1) NOT IN ('2','5')
JOIN {schema}.gc_predio pinf 
    ON pi.predio_informal = pinf.id;
