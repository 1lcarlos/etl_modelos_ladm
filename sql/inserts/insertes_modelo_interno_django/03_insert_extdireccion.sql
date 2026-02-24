-- INSERT para tabla extdireccion (Modelo Interno Django)
-- Migra direcciones desde modelo CCA a modelo interno Django
-- Fecha: 2026-02-06
--
-- Origen: tmp_cca_extdireccion (query cca_extdireccion.sql)
-- Destino: extdireccion (modelo interno Django)
--
-- Diferencias clave con modelo INTERLIS:
--   - PK es 'id' auto-generado (no t_id)
--   - Columna 'seq' en vez de 't_seq'
--   - Dominios usan text_code (no ilicode) y FK apunta a 'id' (no t_id)
--   - No existen columnas extinteresado_ext_direccion_id, gc_terreno_ext_direccion_id,
--     gc_unidadconstruccion_ext_direccion_id
--
-- Dependencias:
--   - Requiere que gc_predio ya este migrado
--
-- IMPORTANTE: Este insert debe ejecutarse DESPUES de gc_predio

INSERT INTO {schema}.extdireccion (
    id,
    --seq,
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
    gc_predio_direccion
)
SELECT
    cca_extdireccion_id,
    -- seq
    --COALESCE(d.t_seq::bigint, 0),

    -- tipo_direccion: Mapeo ilicode (CCA) -> text_code (Django) -> id
    COALESCE(
        (SELECT id FROM {schema}.extdireccion_tipo_direccion
         WHERE text_code = d.tipo_direccion LIMIT 1),
        (SELECT id FROM {schema}.extdireccion_tipo_direccion
         WHERE text_code ILIKE '%' || d.tipo_direccion || '%' LIMIT 1),
        (SELECT id FROM {schema}.extdireccion_tipo_direccion
         WHERE text_code = 'No_Estructurada' LIMIT 1)
    ),

    -- es_direccion_principal
    COALESCE(d.es_direccion_principal, false)::boolean,

    -- localizacion (PointZ SRID 9377)

    CASE
        WHEN d.localizacion IS NOT NULL 
        THEN ST_Force3D(d.localizacion::geometry(pointz, 9377))
        ELSE ST_Force3D(ST_Point(4940023.7497 , 2111479.6705 , 9377))                           
    END,

    -- codigo_postal
    d.codigo_postal,

    -- clase_via_principal: Mapeo ilicode (CCA) -> text_code (Django) -> id
    COALESCE(
        (SELECT id FROM {schema}.extdireccion_clase_via_principal
         WHERE text_code = d.clase_via_principal LIMIT 1),
        (SELECT id FROM {schema}.extdireccion_clase_via_principal
         WHERE text_code ILIKE '%' || d.clase_via_principal || '%' LIMIT 1),
        NULL
    ),

    -- valor_via_principal
    d.valor_via_principal,

    -- letra_via_principal
    d.letra_via_principal,

    -- sector_ciudad: Mapeo ilicode (CCA) -> text_code (Django) -> id
    COALESCE(
        (SELECT id FROM {schema}.extdireccion_sector_ciudad
         WHERE text_code = d.sector_ciudad LIMIT 1),
        (SELECT id FROM {schema}.extdireccion_sector_ciudad
         WHERE text_code ILIKE '%' || d.sector_ciudad || '%' LIMIT 1),
        NULL
    ),

    -- valor_via_generadora
    d.valor_via_generadora,

    -- letra_via_generadora
    d.letra_via_generadora,

    -- numero_predio
    d.numero_predio,

    -- sector_predio: Mapeo ilicode (CCA) -> text_code (Django) -> id
    COALESCE(
        (SELECT id FROM {schema}.extdireccion_sector_predio
         WHERE text_code = d.sector_predio LIMIT 1),
        (SELECT id FROM {schema}.extdireccion_sector_predio
         WHERE text_code ILIKE '%' || d.sector_predio || '%' LIMIT 1),
        NULL
    ),

    -- complemento
    d.complemento,

    -- nombre_predio
    CASE
        WHEN d.nombre_predio IS NULL OR d.nombre_predio = '' THEN 'Sin_direccion'
        ELSE d.nombre_predio
    END,

    -- gc_predio_direccion: Buscar el id del predio migrado
    (SELECT gp.id FROM {schema}.gc_predio gp
     WHERE gp.id = d.id_cca_predio
     LIMIT 1)

FROM tmp_cca_extdireccion d
WHERE d.numero_predial IS NOT NULL
  AND EXISTS (
      -- Solo insertar si el predio ya fue migrado
      SELECT 1 FROM {schema}.gc_predio gp
      WHERE gp.numero_predial = d.numero_predial
  );
