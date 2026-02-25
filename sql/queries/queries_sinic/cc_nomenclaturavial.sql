-- Consulta para extraer datos de la tabla cc_nomenclaturavial

SELECT
    cl.id,
    cl.geometria,
    cl.numero_via,
    cn.text_code as tipo_via
FROM {schema}.cc_nomenclaturavial cl
JOIN {schema}.cc_nomenclaturavial_tipo_via cn ON cn.id = cl.tipo_via;
