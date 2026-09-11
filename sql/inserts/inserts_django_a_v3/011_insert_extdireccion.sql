INSERT INTO {schema}.extdireccion
(   t_id,
    t_basket,
    t_ili_tid,
    tipo_direccion,
    es_direccion_principal,
    localizacion,
    codigo_postal,
    clase_via_principal,
    valor_via_principal,
    letra_via_principal,
    valor_via_generadora,
    letra_via_generadora,
    numero_predio,
    complemento,
    nombre_predio,
    sector_ciudad,
    sector_predio,
    gc_predio_direccion)
SELECT
    --nextval('{schema}.t_ili2db_seq'::regclass),
    te.id_predio::bigint
    ,(SELECT t_id FROM {schema}.t_ili2db_basket LIMIT 1),
    uuid_generate_v4()

    ,COALESCE(
        (SELECT t_id FROM {schema}.extdireccion_tipo_direccion WHERE ilicode = te.tipo_direccion LIMIT 1),
        (SELECT t_id FROM {schema}.extdireccion_tipo_direccion WHERE ilicode = 'No_Estructurada' LIMIT 1)
    )  
     -- es_direccion_principal
    ,COALESCE(te.es_direccion_principal::boolean, False::boolean)
    
    ,CASE
        WHEN te.localizacion IS NOT NULL 
        THEN ST_Force3D(te.localizacion::geometry(pointz, 9377))
        ELSE ST_Force3D(ST_Point(4940027.599 , 2111471.901 , 9377))                           
    END
    
    ,te.codigo_postal

    -- clase_via_principal: Mapeo a extdireccion_clase_via_principal
    ,(SELECT t_id FROM {schema}.extdireccion_clase_via_principal WHERE ilicode = te.clase_via_principal LIMIT 1)

       -- valor_via_principal
    ,te.valor_via_principal

    -- letra_via_principal
    ,te.letra_via_principal

    -- valor_via_generadora
    ,te.valor_via_generadora

    -- letra_via_generadora
    ,te.letra_via_generadora

    -- numero_predio
    ,te.numero_predio

    -- complemento
    ,te.complemento,

    -- nombre_predio   

    CASE
        WHEN te.nombre_predio IN ('', NULL) THEN 'Sin_dirección' 
        ELSE te.nombre_predio
    END,


    -- sector_ciudad: Mapeo a extdireccion_sector_ciudad
    (SELECT t_id FROM {schema}.extdireccion_sector_ciudad WHERE ilicode = te.sector_ciudad LIMIT 1),

    -- sector_predio: Mapeo a extdireccion_sector_predio
    (SELECT t_id FROM {schema}.extdireccion_sector_predio WHERE ilicode = te.sector_predio LIMIT 1),

    -- gc_predio_direccion: referencia al predio
    CASE
        WHEN te.id_predio IS NOT NULL
             AND EXISTS (SELECT 1 FROM {schema}.gc_predio p WHERE p.t_id = te.id_predio)
        THEN te.id_predio
        ELSE NULL
    END

    FROM tmp_extdireccion te
    