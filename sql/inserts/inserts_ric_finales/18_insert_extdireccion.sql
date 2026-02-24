-- INSERT para tabla extdireccion
-- Migra direcciones desde la tabla temporal a la estructura RIC
-- Fecha: 2025-12-18
--
-- Dependencias:
--   - Requiere que existan registros en ric_predio
--   - Usa datos de tmp_extdireccion (query extdireccion.sql)

INSERT INTO ric.extdireccion (
    t_id,
    t_seq,
    tipo_direccion,
    es_direccion_principal,
    localizacion,
    codigo_postal,
    clase_via_principal,
    valor_via_principal,
    letra_via_principal,
    sector_ciudad,
    valor_via_generadora,
    letra_via_generadora,
    numero_predio,
    sector_predio,
    complemento,
    nombre_predio,
    extunidadedificcnfsica_ext_direccion_id,
    extinteresado_ext_direccion_id,
    ric_construccion_ext_direccion_id,
    ric_n_spcjrdcrdsrvcios_ext_direccion_id,
    ric_n_spcjrcndddfccion_ext_direccion_id,
    ric_predio_direccion,
    ric_terreno_ext_direccion_id,
    ric_unidadconstruccion_ext_direccion_id
)
SELECT
    --nextval('ric.t_ili2db_seq'::regclass),
    d.id::bigint,
    -- t_seq
    COALESCE(d.seq::integer, 0),  

    -- tipo_direccion (NOT NULL): Mapeo a extdireccion_tipo_direccion
    COALESCE(
        (SELECT t_id FROM ric.extdireccion_tipo_direccion
         WHERE ilicode ILIKE '%' || d.tipo_direccion || '%'
         LIMIT 1),
        (SELECT t_id FROM ric.extdireccion_tipo_direccion
         WHERE ilicode = 'No_Estructurada'
         LIMIT 1)
    ),

    -- es_direccion_principal
    CASE
        WHEN d.es_direccion_principal = 'true' THEN TRUE
        WHEN d.es_direccion_principal = 'false' THEN FALSE
        ELSE TRUE
    END,

    -- localizacion (PointZ)
    CASE
        WHEN d.localizacion IS NOT NULL 
        THEN ST_Force3D(d.localizacion::geometry(pointz, 9377))
        ELSE ST_Force3D(ST_Point(4839656.153815 , 2064698.290845 , 9377))                           
    END,

    -- codigo_postal
    d.codigo_postal,

    -- clase_via_principal: Mapeo a extdireccion_clase_via_principal
    (SELECT t_id FROM ric.extdireccion_clase_via_principal
     WHERE ilicode ILIKE '%' || d.clase_via_principal || '%'
     LIMIT 1),

    -- valor_via_principal
    d.valor_via_principal,

    -- letra_via_principal
    d.letra_via_principal,

    -- sector_ciudad: Mapeo a extdireccion_sector_ciudad
    (SELECT t_id FROM ric.extdireccion_sector_ciudad
     WHERE ilicode ILIKE '%' || d.sector_ciudad || '%'
     LIMIT 1),

    -- valor_via_generadora
    d.valor_via_generadora,

    -- letra_via_generadora
    d.letra_via_generadora,

    -- numero_predio
    d.numero_predio,

    -- sector_predio: Mapeo a extdireccion_sector_predio
    (SELECT t_id FROM ric.extdireccion_sector_predio
     WHERE ilicode ILIKE '%' || d.sector_predio || '%'
     LIMIT 1),

    -- complemento
    d.complemento,

    -- nombre_predio
    CASE
        WHEN d.nombre_predio IN ('', NULL) THEN 'Sin_dirección' 
        ELSE d.nombre_predio
    END,

    -- extunidadedificcnfsica_ext_direccion_id (NULL)
    NULL,

    -- extinteresado_ext_direccion_id (NULL)
    NULL,

    -- ric_construccion_ext_direccion_id (NULL)
    NULL,

    -- ric_n_spcjrdcrdsrvcios_ext_direccion_id (NULL)
    NULL,

    -- ric_n_spcjrcndddfccion_ext_direccion_id (NULL)
    NULL,

    -- ric_predio_direccion: Buscar el t_id del predio en ric_predio
    (SELECT rp.t_id
     FROM ric.ric_predio rp
     WHERE rp.local_id::integer = d.gc_predio_direccion::integer
     LIMIT 1),

    -- ric_terreno_ext_direccion_id (NULL)
    NULL,

    -- ric_unidadconstruccion_ext_direccion_id (NULL)
    NULL

FROM tmp_extdireccion d
WHERE d.gc_predio_direccion IS NOT NULL
  AND EXISTS (
      SELECT 1 FROM ric.ric_predio rp
      WHERE rp.local_id::integer = d.gc_predio_direccion::integer
  );
