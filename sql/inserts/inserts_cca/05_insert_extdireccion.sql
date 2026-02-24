-- INSERT para tabla extdireccion
-- Migra direcciones desde modelo CCA a modelo GC
-- Fecha: 2026-02-05
--
-- Dependencias:
--   - Usa datos de tmp_cca_extdireccion (query cca_extdireccion.sql)
--   - Requiere que gc_predio ya este migrado
--
-- IMPORTANTE: Este insert debe ejecutarse DESPUES de gc_predio

INSERT INTO {schema}.extdireccion (
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
    extinteresado_ext_direccion_id,
    gc_terreno_ext_direccion_id,
    gc_unidadconstruccion_ext_direccion_id,
    gc_predio_direccion
)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),

    -- t_seq
    COALESCE(d.t_seq::integer, 0),

    -- tipo_direccion (NOT NULL): Mapeo a extdireccion_tipo_direccion
    COALESCE(
        (SELECT t_id FROM {schema}.extdireccion_tipo_direccion
         WHERE ilicode = d.tipo_direccion LIMIT 1),
        (SELECT t_id FROM {schema}.extdireccion_tipo_direccion
         WHERE ilicode ILIKE '%' || d.tipo_direccion || '%' LIMIT 1),
        (SELECT t_id FROM {schema}.extdireccion_tipo_direccion
         WHERE ilicode = 'No_Estructurada' LIMIT 1)
    ),

    -- es_direccion_principal
    COALESCE(d.es_direccion_principal, true),

    -- localizacion (PointZ SRID 9377)
    CASE
        WHEN d.localizacion IS NOT NULL THEN
            ST_Force3D(d.localizacion)
        ELSE NULL
    END,

    -- codigo_postal
    d.codigo_postal,

    -- clase_via_principal: Mapeo a extdireccion_clase_via_principal
    (SELECT t_id FROM {schema}.extdireccion_clase_via_principal
     WHERE ilicode = d.clase_via_principal
        OR ilicode ILIKE '%' || d.clase_via_principal || '%'
     LIMIT 1),

    -- valor_via_principal
    d.valor_via_principal,

    -- letra_via_principal
    d.letra_via_principal,

    -- sector_ciudad: Mapeo a extdireccion_sector_ciudad
    (SELECT t_id FROM {schema}.extdireccion_sector_ciudad
     WHERE ilicode = d.sector_ciudad
        OR ilicode ILIKE '%' || d.sector_ciudad || '%'
     LIMIT 1),

    -- valor_via_generadora
    d.valor_via_generadora,

    -- letra_via_generadora
    d.letra_via_generadora,

    -- numero_predio
    d.numero_predio,

    -- sector_predio: Mapeo a extdireccion_sector_predio
    (SELECT t_id FROM {schema}.extdireccion_sector_predio
     WHERE ilicode = d.sector_predio
        OR ilicode ILIKE '%' || d.sector_predio || '%'
     LIMIT 1),

    -- complemento
    d.complemento,

    -- nombre_predio
    CASE
        WHEN d.nombre_predio IS NULL OR d.nombre_predio = '' THEN 'Sin_direccion'
        ELSE d.nombre_predio
    END,

    -- extinteresado_ext_direccion_id (NULL - no aplica para direcciones de predio)
    NULL,

    -- gc_terreno_ext_direccion_id (NULL - no aplica para direcciones de predio)
    NULL,

    -- gc_unidadconstruccion_ext_direccion_id (NULL - no aplica para direcciones de predio)
    NULL,

    -- gc_predio_direccion: Buscar el t_id del predio migrado
    (SELECT gp.t_id FROM {schema}.gc_predio gp
     WHERE gp.numero_predial = d.numero_predial
     LIMIT 1)

FROM tmp_cca_extdireccion d
WHERE d.numero_predial IS NOT NULL
  AND EXISTS (
      -- Solo insertar si el predio ya fue migrado
      SELECT 1 FROM {schema}.gc_predio gp
      WHERE gp.numero_predial = d.numero_predial
  );
