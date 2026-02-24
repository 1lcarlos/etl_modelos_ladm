-- Query para extraer datos de gc_construccion
-- Fecha: 2025-12-18

SELECT
    c.id,
    COALESCE(c.espacio_de_nombres, 'GC_CONSTRUCCION') as espacio_de_nombres,
    COALESCE(c.local_id, c.id::varchar) as local_id,
    CASE
        WHEN c.comienzo_vida_util_version IS NULL THEN now()
        ELSE c.comienzo_vida_util_version
    END as comienzo_vida_util_version,
    c.fin_vida_util_version,
    c.identificador,
    c.codigo,
    COALESCE(c.etiqueta, '0') as etiqueta,
    COALESCE(c.numero_pisos, 1) as numero_pisos,
    COALESCE(c.numero_sotanos, 0) as numero_sotanos,
    COALESCE(c.numero_mezanines, 0) as numero_mezanines,
    COALESCE(c.numero_semisotanos, 0) as numero_semisotanos,
    c.anio_construccion,
    COALESCE(c.area, 1) as area_construccion,
    c.altura,
    c.observacion as observaciones,
    c.geometria,
    ct.text_code as tipo_construccion,
    dt.text_code as tipo_dominio
FROM {schema}.gc_construccion c
LEFT JOIN {schema}.gc_construcciontipo ct ON c.tipo_construccion = ct.id
LEFT JOIN {schema}.gc_dominioconstrucciontipo dt ON c.tipo_dominio = dt.id
WHERE c.geometria IS NOT NULL;
