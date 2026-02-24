-- INSERT para tabla gc_predio (Modelo Interno Django)
-- Migra predios desde modelo CCA a modelo interno Django
-- Fecha: 2026-02-06
--
-- Origen: tmp_cca_predio (query cca_predio.sql)
-- Destino: gc_predio (modelo interno Django)
--
-- Diferencias clave con modelo INTERLIS:
--   - PK es 'id' auto-generado (no t_id)
--   - No existe t_ili_tid
--   - Dominios usan text_code (no ilicode)
--
-- IMPORTANTE: Este insert debe ejecutarse PRIMERO (sin dependencias)

INSERT INTO {schema}.gc_predio (
    id,
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
    nupre,
    interrelacionado,
    nupre_fmi,
    --area,
    --area_construida,
    nombre,
    rectificacion_efecto_registral,
    categoria_suelo,
    clase_suelo,
    condicion_predio,
    destinacion_economica,
    tipo
)
SELECT
    --id_predio
    d.cca_predio_id,

    -- espacio_de_nombres
    'GC_PREDIO_CCA',

    -- local_id: Usar cca_predio_id o numero_predial como identificador
    COALESCE(d.cca_predio_id::varchar, d.numero_predial),

    -- comienzo_vida_util_version
    NOW(),

    -- fin_vida_util_version
    NULL,

    -- departamento (NOT NULL)
    d.departamento,

    -- municipio (NOT NULL)
    d.municipio,

    -- tiene_fmi (boolean)
    d.tiene_fmi::boolean,

    -- codigo_orip
    d.codigo_orip,

    -- matricula_inmobiliaria
    d.matricula_inmobiliaria::numeric,

    -- numero_predial
    d.numero_predial,

    -- numero_predial_anterior
    d.numero_predial_anterior,

    -- nupre
    d.nupre,

    -- interrelacionado (valor por defecto false)
    false::boolean,

    -- nupre_fmi (valor por defecto false)
    false::boolean,

    -- area
    --COALESCE(d.area_terreno, 0),

    -- area_construida (no disponible en CCA a nivel predio)
    --COALESCE(d.area_construida, 0),

    -- nombre
    d.nombre,

    -- rectificacion_efecto_registral (valor por defecto false)
    false::boolean,

    -- categoria_suelo: Mapeo ilicode (CCA) -> text_code (Django)
    COALESCE(
        (SELECT id FROM {schema}.gc_categoriasuelotipo
         WHERE text_code = d.categoria_suelo LIMIT 1),
        (SELECT id FROM {schema}.gc_categoriasuelotipo
         WHERE text_code ILIKE '%' || d.categoria_suelo || '%' LIMIT 1),
        NULL
    ),

    -- clase_suelo: Mapeo ilicode (CCA) -> text_code (Django)
    COALESCE(
        (SELECT id FROM {schema}.gc_clasesuelotipo
         WHERE text_code = d.clase_suelo LIMIT 1),
        (SELECT id FROM {schema}.gc_clasesuelotipo
         WHERE text_code ILIKE '%' || d.clase_suelo || '%' LIMIT 1),
        NULL
    ),

    -- condicion_predio: Mapeo ilicode (CCA) -> text_code (Django)
    COALESCE(
        (SELECT id FROM {schema}.gc_condicionprediotipo
         WHERE text_code = d.condicion_predio LIMIT 1),
        (SELECT id FROM {schema}.gc_condicionprediotipo
         WHERE text_code ILIKE '%' || d.condicion_predio || '%' LIMIT 1),
        NULL
    ),

    -- destinacion_economica: Mapeo ilicode (CCA) -> text_code (Django)
    COALESCE(
        (SELECT id FROM {schema}.gc_destinacioneconomicatipo
         WHERE text_code = d.destinacion_economica LIMIT 1),
        (SELECT id FROM {schema}.gc_destinacioneconomicatipo
         WHERE text_code ILIKE '%' || d.destinacion_economica || '%' LIMIT 1),
        NULL
    ),

    -- tipo: Mapeo ilicode (CCA) -> text_code (Django)
    COALESCE(
        (SELECT id FROM {schema}.gc_prediotipo
         WHERE text_code = d.tipo_predio LIMIT 1),
        (SELECT id FROM {schema}.gc_prediotipo
         WHERE text_code ILIKE '%' || d.tipo_predio || '%' LIMIT 1),
        NULL
    )

FROM tmp_cca_predio d
WHERE d.numero_predial IS NOT NULL
  AND d.departamento IS NOT NULL
  AND d.municipio IS NOT NULL;
