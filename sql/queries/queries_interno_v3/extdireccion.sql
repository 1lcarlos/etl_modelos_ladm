-- Consulta para extraer datos de extdireccion del modelo interno_v3
-- Dirección: interno_v3 → modelo interno Django
-- Se extraen los ilicode de las tablas de dominio y el local_id del predio

SELECT
    ex.t_id,
    ex.t_seq as seq,
    ex.es_direccion_principal::text,
    ex.codigo_postal,
    ex.valor_via_principal,
    ex.letra_via_principal,
    ex.valor_via_generadora,
    ex.letra_via_generadora,
    ex.numero_predio,
    ex.complemento,
    ex.nombre_predio,
    ex.localizacion,
    ecvp.ilicode as clase_via_principal,
    p.local_id as gc_predio_direccion,
    esc.ilicode as sector_ciudad,
    esp.ilicode as sector_predio,
    etd.ilicode as tipo_direccion
FROM {schema}.extdireccion ex
LEFT JOIN {schema}.extdireccion_tipo_direccion etd ON etd.t_id = ex.tipo_direccion
LEFT JOIN {schema}.extdireccion_clase_via_principal ecvp ON ecvp.t_id = ex.clase_via_principal
LEFT JOIN {schema}.extdireccion_sector_ciudad esc ON esc.t_id = ex.sector_ciudad
LEFT JOIN {schema}.extdireccion_sector_predio esp ON esp.t_id = ex.sector_predio
LEFT JOIN {schema}.gc_predio p ON p.t_id = ex.gc_predio_direccion;
