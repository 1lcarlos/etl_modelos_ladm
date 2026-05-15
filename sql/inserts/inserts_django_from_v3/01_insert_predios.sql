-- Inserción de predios en modelo Django desde interno_v3
-- Dirección: interno_v3 → modelo interno Django
--
-- Mapeo de dominios: ilicode (interno_v3) → text_code (Django)
-- Coincidencia de 2 niveles: exacta, luego parcial ILIKE
-- El id se genera automáticamente por la secuencia gc_predio_id_seq de Django

INSERT INTO {schema}.gc_predio (
    espacio_de_nombres,
    local_id,
    comienzo_vida_util_version,
    fin_vida_util_version,
    departamento,
    municipio,
    codigo_orip,
    matricula_inmobiliaria,
    numero_predial,
    numero_predial_anterior,
    nupre,
    area,
    area_construida,
    nombre,
    tipo,
    categoria_suelo,
    clase_suelo,
    condicion_predio,
    destinacion_economica
)
SELECT
    p.espacio_de_nombres,
    p.local_id,
    p.comienzo_vida_util_version,
    p.fin_vida_util_version,
    p.departamento,
    p.municipio,
    p.codigo_orip,
    p.matricula_inmobiliaria,
    p.numero_predial,
    p.numero_predial_anterior,
    p.nupre,
    p.area::numeric,
    p.area_construida::numeric,
    p.nombre,

    -- tipo (gc_prediotipo): ilicode → text_code con coincidencia parcial
    -- En interno_v3 tipo_predio usa ilicode (ej: 'Predio.Privado'), en Django text_code (ej: 'Privado')
    COALESCE(
        (SELECT id FROM {schema}.gc_prediotipo WHERE text_code = p.tipo_predio LIMIT 1),
        (SELECT id FROM {schema}.gc_prediotipo WHERE p.tipo_predio ILIKE '%' || text_code || '%' LIMIT 1),
        (SELECT id FROM {schema}.gc_prediotipo WHERE text_code = 'Privado' LIMIT 1)
    ) AS tipo,

    -- categoria_suelo: coincidencia exacta (puede ser NULL)
    COALESCE(
        (SELECT id FROM {schema}.gc_categoriasuelotipo WHERE text_code = p.categoria_suelo LIMIT 1),
        (SELECT id FROM {schema}.gc_categoriasuelotipo WHERE p.categoria_suelo ILIKE '%' || text_code || '%' LIMIT 1)
    ) AS categoria_suelo,

    -- clase_suelo: coincidencia exacta + fallback por numero_predial (pos 6-7)
    COALESCE(
        (SELECT id FROM {schema}.gc_clasesuelotipo WHERE text_code = p.clase_suelo LIMIT 1),
        (SELECT id FROM {schema}.gc_clasesuelotipo WHERE p.clase_suelo ILIKE '%' || text_code || '%' LIMIT 1),
        CASE
            WHEN SUBSTRING(p.numero_predial FROM 6 FOR 2) = '00'
            THEN (SELECT id FROM {schema}.gc_clasesuelotipo WHERE text_code = 'Rural' LIMIT 1)
            ELSE (SELECT id FROM {schema}.gc_clasesuelotipo WHERE text_code = 'Urbano' LIMIT 1)
        END
    ) AS clase_suelo,

    -- condicion_predio: coincidencia exacta + fallback NPH
    COALESCE(
        (SELECT id FROM {schema}.gc_condicionprediotipo WHERE text_code = p.condicion_predio LIMIT 1),
        (SELECT id FROM {schema}.gc_condicionprediotipo WHERE p.condicion_predio ILIKE '%' || text_code || '%' LIMIT 1),
        (SELECT id FROM {schema}.gc_condicionprediotipo WHERE text_code = 'NPH' LIMIT 1)
    ) AS condicion_predio,

    -- destinacion_economica: coincidencia exacta + fallback por numero_predial (pos 6-7)
    COALESCE(
        (SELECT id FROM {schema}.gc_destinacioneconomicatipo WHERE text_code = p.destinacion_economica LIMIT 1),
        (SELECT id FROM {schema}.gc_destinacioneconomicatipo WHERE p.destinacion_economica ILIKE '%' || text_code || '%' LIMIT 1),
        CASE
            WHEN SUBSTRING(p.numero_predial FROM 6 FOR 2) = '00'
            THEN (SELECT id FROM {schema}.gc_destinacioneconomicatipo WHERE text_code = 'Agricola' LIMIT 1)
            ELSE (SELECT id FROM {schema}.gc_destinacioneconomicatipo WHERE text_code = 'Habitacional' LIMIT 1)
        END
    ) AS destinacion_economica

FROM tmp_predio p;
