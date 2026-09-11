SELECT 
ex.id, seq, es_direccion_principal::text, 
codigo_postal, valor_via_principal, letra_via_principal, 
valor_via_generadora, letra_via_generadora, numero_predio, 
complemento, nombre_predio, localizacion, 
ecvp.text_code as clase_via_principal, gc_predio_direccion, esc.text_code as sector_ciudad, 
esp.text_code as sector_predio, etd.text_code as tipo_direccion
FROM {schema}.extdireccion ex
LEFT JOIN {schema}.extdireccion_tipo_direccion etd on etd.id = ex.tipo_direccion
LEFT JOIN {schema}.extdireccion_clase_via_principal ecvp on ecvp.id = ex.clase_via_principal
LEFT JOIN {schema}.extdireccion_sector_ciudad esc on esc.id = ex.sector_ciudad
LEFT JOIN {schema}.extdireccion_sector_predio esp on esp.id = ex.sector_predio
LEFT JOIN {schema}.gc_predio p on p.id = ex.gc_predio_direccion;