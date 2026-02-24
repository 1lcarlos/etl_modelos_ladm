-- Consulta para extraer datos de cca_construccion para migrar a gc_construccion
-- Origen: Modelo CCA (cca_construccion)
-- Destino: gc_construccion (modelo interno Django)
-- Fecha: 2026-02-06
--
-- Notas:
-- 1. cca_construccion tiene relacion directa con cca_predio via campo 'predio'
-- 2. Los dominios tipo_construccion y tipo_dominio se extraen con ilicode para mapeo
-- 3. La geometria debe ser MultiPolygonZ SRID 9377
-- 4. El codigo se genera a partir del numero_predial del predio relacionado

SELECT
    c.t_id as cca_construccion_id,
    c.t_ili_tid,
    c.identificador,
    c.numero_pisos,
    c.numero_sotanos,
    c.numero_mezanines,
    c.numero_semisotanos,
    c.area_construccion_alfanumerica,
    c.area_construccion_digital,
    c.anio_construccion,
    c.valor_referencia_construccion,
    c.etiqueta,
    c.altura,
    c.observaciones,
    c.geometria,

    -- Relacion con predio
    c.predio as cca_predio_id,

    -- Datos del predio para codigo y relaciones
    p.numero_predial,
    p.nupre,

    -- Dominios con ilicode para mapeo en destino
    ct.ilicode as tipo_construccion,
    dt.ilicode as tipo_dominio

FROM {schema}.cca_construccion c
INNER JOIN {schema}.cca_predio p ON c.predio = p.t_id
LEFT JOIN {schema}.cca_construcciontipo ct ON c.tipo_construccion = ct.t_id
LEFT JOIN {schema}.cca_dominioconstrucciontipo dt ON c.tipo_dominio = dt.t_id;
