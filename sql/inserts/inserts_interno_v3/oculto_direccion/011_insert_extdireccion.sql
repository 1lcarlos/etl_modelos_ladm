INSERT INTO {schema}.extdireccion
(t_id, t_seq, tipo_direccion, 
es_direccion_principal, localizacion, codigo_postal, 
clase_via_principal, valor_via_principal, letra_via_principal, 
sector_ciudad, valor_via_generadora, letra_via_generadora, 
numero_predio, sector_predio, complemento, 
nombre_predio, gc_predio_direccion)
SELECT
    --nextval('{schema}.t_ili2db_seq'::regclass),
    te.id::bigint,
    COALESCE(t.seq::integer, 0),  

  COALESCE(
        (SELECT t_id FROM {schema}.extdireccion_tipo_direccion
         WHERE ilicode ILIKE '%' || te.tipo_direccion || '%'
         LIMIT 1),
        (SELECT t_id FROM {schema}.extdireccion_tipo_direccion
         WHERE ilicode = 'No_Estructurada'
         LIMIT 1)
    ),

    CASE
        WHEN te.es_direccion_principal = 'true' THEN TRUE
        WHEN te.es_direccion_principal = 'false' THEN FALSE
        ELSE TRUE
    END,

    CASE
        WHEN te.localizacion IS NOT NULL 
        THEN ST_Force3D(te.localizacion::geometry(pointz, 9377))
        ELSE ST_Force3D(ST_Point(4818794.6079  , 2050800.8250 , 9377))                     
    END,
    
    te.codigo_postal,

    (SELECT t_id FROM {schema}.extdireccion_clase_via_principal
     WHERE ilicode ILIKE '%' || te.clase_via_principal || '%'
     LIMIT 1),

    te.valor_via_principal,
    te.letra_via_principal,

     -- sector_ciudad: Mapeo a extdireccion_sector_ciudad
    (SELECT t_id FROM ric.extdireccion_sector_ciudad
     WHERE ilicode ILIKE '%' || d.sector_ciudad || '%'
     LIMIT 1),
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
    left JOIN {schema}.gc_predio p ON te.id::text = p.local_id
    WHERE te.gc_predio_direccion IS NOT NULL
    AND EXISTS (
      SELECT 1 FROM {schema}.gp_predio rp
      WHERE rp.local_id::integer = te.gc_predio_direccion::integer
  );;