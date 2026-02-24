-- Query para extraer datos de extdireccion para migrar
-- Origen: {schema}.extdireccion
-- Destino: sinic2.extdireccion
-- Fecha: 2026-02-02

SELECT DISTINCT ON (ed.id)
    ed.id,
    'EXTDIRECCION' as espacio_de_nombres,
    ed.id::varchar as local_id,
    td.text_code as tipo_direccion,
    ed.es_direccion_principal,
    ed.localizacion,
    ed.codigo_postal,
    cvp.text_code as clase_via_principal,
    ed.valor_via_principal,
    ed.letra_via_principal,
    ed.valor_via_generadora,
    ed.letra_via_generadora,
    ed.numero_predio,
    ed.complemento,
    ed.nombre_predio,
    sc.text_code as sector_ciudad,
    sp.text_code as sector_predio,
    ed.gc_predio_direccion as predio_id
FROM {schema}.extdireccion ed
LEFT JOIN {schema}.extdireccion_tipo_direccion td ON ed.tipo_direccion = td.id
LEFT JOIN {schema}.extdireccion_clase_via_principal cvp ON ed.clase_via_principal = cvp.id
LEFT JOIN {schema}.extdireccion_sector_ciudad sc ON ed.sector_ciudad = sc.id
LEFT JOIN {schema}.extdireccion_sector_predio sp ON ed.sector_predio = sp.id
ORDER BY ed.id;
