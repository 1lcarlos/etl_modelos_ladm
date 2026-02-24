-- INSERT para tabla gc_predio
-- Migra predios desde modelo CCA a modelo GC
-- Fecha: 2026-02-05
--
-- Dependencias:
--   - Usa datos de tmp_cca_predio (query cca_predio.sql)
--   - Requiere tablas de dominio en esquema destino
--
-- IMPORTANTE: Este insert debe ejecutarse ANTES de terrenos, direcciones y col_uebaunit

INSERT INTO {schema}.gc_predio (
    t_id,
    t_ili_tid,
    espacio_de_nombres,
    local_id,
    comienzo_vida_util_version,
    fin_vida_util_version,
    departamento,
    municipio,
    tiene_fmi,
    codigo_orip,
    matricula_inmobiliaria,
    numero_predial,
    numero_predial_anterior,
    codigo_homologado,
    nupre,
    interrelacionado,
    nupre_fmi,
    area,
    area_construida,
    nombre,
    rectificacion_efecto_registral,
    categoria_suelo,
    clase_suelo,
    condicion_predio,
    destinacion_economica,
    tipo
)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    --d.cca_predio_id,
    uuid_generate_v4(),

    -- espacio_de_nombres (NOT NULL)
    'GC_PREDIO_CCA',

    -- local_id (NOT NULL): Usar numero_predial o id original
    COALESCE(d.cca_predio_id::varchar, d.numero_predial ),

    -- comienzo_vida_util_version (NOT NULL)
    NOW(),

    -- fin_vida_util_version
    NULL,

    -- departamento (NOT NULL)
    d.departamento,

    -- municipio (NOT NULL)
    d.municipio,

    -- tiene_fmi (boolean)
    d.tiene_fmi::bolean,

    -- codigo_orip
    d.codigo_orip,

    -- matricula_inmobiliaria
    d.matricula_inmobiliaria::numeric,

    -- numero_predial
    d.numero_predial,

    -- numero_predial_anterior
    d.numero_predial_anterior,

    -- codigo_homologado
    d.nupre,  

    -- nupre
    d.nupre,

    -- interrelacionado (valor por defecto false)
    false,
   
    -- area
    COALESCE(d.area, 0),

    -- area_construida
    d.area_construida,

    -- nombre (no existe en CCA, usar NULL o codigo_homologado)
    d.codigo_homologado,

    -- rectificacion_efecto_registral (valor por defecto false)
    false,

    -- categoria_suelo: Mapeo de CCA a gc_categoriasuelo
    COALESCE(
        (SELECT t_id FROM {schema}.gc_categoriasuelo
         WHERE ilicode = d.categoria_suelo LIMIT 1),
        (SELECT t_id FROM {schema}.gc_categoriasuelo
         WHERE ilicode ILIKE '%' || d.categoria_suelo || '%' LIMIT 1),
        NULL
    ),

    -- clase_suelo: Mapeo de CCA a gc_clasesuelotipo
    COALESCE(
        (SELECT t_id FROM {schema}.gc_clasesuelotipo
         WHERE ilicode = d.clase_suelo LIMIT 1),
        (SELECT t_id FROM {schema}.gc_clasesuelotipo
         WHERE ilicode ILIKE '%' || d.clase_suelo || '%' LIMIT 1),
        NULL
    ),

    -- condicion_predio: Mapeo de CCA a gc_condicionprediotipo
    COALESCE(
        (SELECT t_id FROM {schema}.gc_condicionprediotipo
         WHERE ilicode = d.condicion_predio LIMIT 1),
        (SELECT t_id FROM {schema}.gc_condicionprediotipo
         WHERE ilicode ILIKE '%' || d.condicion_predio || '%' LIMIT 1),
        NULL
    ),

    -- destinacion_economica: Mapeo de CCA a gc_destinacioneconomicatipo
    COALESCE(
        (SELECT t_id FROM {schema}.gc_destinacioneconomicatipo
         WHERE ilicode = d.destinacion_economica LIMIT 1),
        (SELECT t_id FROM {schema}.gc_destinacioneconomicatipo
         WHERE ilicode ILIKE '%' || d.destinacion_economica || '%' LIMIT 1),
        NULL
    ),

    -- tipo: Mapeo de CCA a gc_prediotipo
    COALESCE(
        (SELECT t_id FROM {schema}.gc_prediotipo
         WHERE ilicode = d.tipo_predio LIMIT 1),
        (SELECT t_id FROM {schema}.gc_prediotipo
         WHERE ilicode ILIKE '%' || d.tipo_predio || '%' LIMIT 1),
        NULL
    )

FROM tmp_cca_predio d
WHERE d.numero_predial IS NOT NULL
  AND d.departamento IS NOT NULL
  AND d.municipio IS NOT NULL;
