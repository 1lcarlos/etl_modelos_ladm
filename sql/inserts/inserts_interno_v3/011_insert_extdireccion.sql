INSERT INTO {schema}.extdireccion
(t_id, 
--t_seq, 
tipo_direccion, 
es_direccion_principal, localizacion, codigo_postal, 
clase_via_principal, valor_via_principal, letra_via_principal, 
sector_ciudad, valor_via_generadora, letra_via_generadora, 
numero_predio, sector_predio, complemento, 
nombre_predio, gc_predio_direccion)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    --te.seq,
    COALESCE(etd.t_id,(SELECT t_id FROM {schema}.extdireccion_tipo_direccion WHERE ilicode = 'Estructurada')) as tipo_direccion,
    te.es_direccion_principal::boolean,
    te.localizacion,
    te.codigo_postal,
    ecvp.t_id as clase_via_principal,
    te.valor_via_principal,
    te.letra_via_principal,
    esc.t_id as sector_ciudad,
    te.valor_via_generadora,
    te.letra_via_generadora,
    te.numero_predio,
    esp.t_id as sector_predio,
    te.complemento,
    te.nombre_predio,
    p.t_id as gc_predio_direccion
    FROM tmp_extdireccion te
    left JOIN {schema}.extdireccion_tipo_direccion etd ON te.tipo_direccion = etd.ilicode
    left JOIN {schema}.extdireccion_clase_via_principal ecvp ON te.clase_via_principal = ecvp.ilicode
    left JOIN {schema}.extdireccion_sector_ciudad esc ON te.sector_ciudad = esc.ilicode
    left JOIN {schema}.extdireccion_sector_predio esp ON te.sector_predio = esp.ilicode
    left JOIN {schema}.gc_predio p ON te.gc_predio_direccion::text = p.local_id;