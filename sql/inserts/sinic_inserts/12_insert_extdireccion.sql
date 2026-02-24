-- INSERT para tabla extdireccion
-- Migra direcciones desde la tabla temporal a SINIC
-- Fecha: 2026-02-02
--
-- Dependencias:
--   - Requiere que sinic_predio ya este migrado
--   - Usa datos de tmp_sinic_extdireccion (query extdireccion.sql)

INSERT INTO {schema}.extdireccion (
    t_id,
    --t_basket,
    t_seq,
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
    sinic_predio_direccion
)
SELECT
    ed.id::bigint,
    --(SELECT t_id FROM {schema}.t_ili2db_basket LIMIT 1),
    NULL,

    -- tipo_direccion: Mapeo a extdireccion_tipo_direccion
    COALESCE(
        (SELECT t_id FROM {schema}.extdireccion_tipo_direccion WHERE ilicode = ed.tipo_direccion LIMIT 1),
        (SELECT t_id FROM {schema}.extdireccion_tipo_direccion WHERE ilicode = 'No_Estructurada' LIMIT 1)
    ),

    -- es_direccion_principal
    COALESCE(ed.es_direccion_principal::boolean, False::boolean),

    -- localizacion
     CASE
        WHEN ed.localizacion IS NOT NULL 
        THEN ST_Force3D(ed.localizacion::geometry(pointz, 9377))
        ELSE ST_Force3D(ST_Point(4839656.153815 , 2064698.290845 , 9377))                           
    END,

    -- codigo_postal
    ed.codigo_postal,

    -- clase_via_principal: Mapeo a extdireccion_clase_via_principal
    (SELECT t_id FROM {schema}.extdireccion_clase_via_principal WHERE ilicode = ed.clase_via_principal LIMIT 1),

    -- valor_via_principal
    ed.valor_via_principal,

    -- letra_via_principal
    ed.letra_via_principal,

    -- valor_via_generadora
    ed.valor_via_generadora,

    -- letra_via_generadora
    ed.letra_via_generadora,

    -- numero_predio
    ed.numero_predio,

    -- complemento
    ed.complemento,

    -- nombre_predio
    ed.nombre_predio,

    -- sector_ciudad: Mapeo a extdireccion_sector_ciudad
    (SELECT t_id FROM {schema}.extdireccion_sector_ciudad WHERE ilicode = ed.sector_ciudad LIMIT 1),

    -- sector_predio: Mapeo a extdireccion_sector_predio
    (SELECT t_id FROM {schema}.extdireccion_sector_predio WHERE ilicode = ed.sector_predio LIMIT 1),

    -- sinic_predio_direccion: referencia al predio
    CASE
        WHEN ed.predio_id IS NOT NULL
             AND EXISTS (SELECT 1 FROM {schema}.sinic_predio p WHERE p.t_id = ed.predio_id)
        THEN ed.predio_id
        ELSE NULL
    END

FROM tmp_extdireccion ed;
