-- Consulta para extraer datos de extdireccion del modelo CCA
-- Origen: Modelo CCA (extdireccion)
-- Destino: extdireccion (modelo GC)
-- Fecha: 2026-02-05
--
-- Notas:
-- 1. Los campos de dominio se extraen con ilicode para mapeo en destino
-- 2. cca_predio_direccion es la FK hacia cca_predio

SELECT
    ex.t_id as cca_extdireccion_id,
    ex.t_seq,
    ex.es_direccion_principal,
    ex.localizacion,
    ex.codigo_postal,
    ex.valor_via_principal,
    ex.letra_via_principal,
    ex.valor_via_generadora,
    ex.letra_via_generadora,
    ex.numero_predio,
    ex.complemento,
    ex.nombre_predio,

    -- FK hacia cca_predio
    ex.cca_predio_direccion as id_cca_predio,

    -- Dominios con ilicode para mapeo
    etd.ilicode as tipo_direccion,
    ecvp.ilicode as clase_via_principal,
    esc.ilicode as sector_ciudad,
    esp.ilicode as sector_predio,

    -- Obtener numero_predial del predio para relacionar en destino
    p.numero_predial

FROM {schema}.extdireccion ex
LEFT JOIN {schema}.extdireccion_tipo_direccion etd ON ex.tipo_direccion = etd.t_id
LEFT JOIN {schema}.extdireccion_clase_via_principal ecvp ON ex.clase_via_principal = ecvp.t_id
LEFT JOIN {schema}.extdireccion_sector_ciudad esc ON ex.sector_ciudad = esc.t_id
LEFT JOIN {schema}.extdireccion_sector_predio esp ON ex.sector_predio = esp.t_id
LEFT JOIN {schema}.cca_predio p ON ex.cca_predio_direccion = p.t_id
WHERE ex.cca_predio_direccion IS NOT NULL;
