-- Consulta corregida para inserción de predios
-- Versión: Usa coincidencias EXACTAS para evitar duplicación de registros
-- Fecha: 2025-12-14
--
-- Cambios principales:
-- 1. destinacion_economica: Coincidencia EXACTA (diferencia Pecuario vs Agropecuario)
-- 2. condicion_predio: Coincidencia EXACTA
-- 3. clase_suelo: Coincidencia EXACTA
-- 4. tipo_predio: Subquery con LIMIT 1 (coincidencia parcial: Privado -> Predio.Privado)
-- 5. Todos los JOINs usan subqueries para evitar multiplicación de registros

INSERT INTO {schema}.gc_predio (
    t_id,
    t_ili_tid,
    departamento,
    municipio,
    codigo_orip,
    matricula_inmobiliaria,
    numero_predial_nacional,
    codigo_homologado,
    nupre,
    tipo_predio,
    condicion_predio,
    destinacion_economica,
    area_catastral_terreno,
    area_registral_m2,
    vigencia_actualizacion_catastral,
    estado,
    categoria_suelo,
    clase_suelo,
    numero_predial_anterior,
    area_construida,
    nombre,
    tipo,
    comienzo_vida_util_version,
    fin_vida_util_version,
    espacio_de_nombres,
    local_id
)
SELECT
    nextval('{schema}.t_ili2db_seq'::regclass),
    uuid_generate_v4(),
    p.departamento,
    p.municipio,
    p.codigo_orip,
    p.matricula_inmobiliaria::numeric,
    p.numero_predial_nacional,
    p.nupre,  -- codigo_homologado
    p.nupre,  -- nupre

    -- tipo_predio: Coincidencia parcial con subquery (Privado -> Predio.Privado)
    COALESCE(
        (SELECT t_id FROM {schema}.gc_prediotipo WHERE ilicode ILIKE '%' || p.tipo_predio || '%' LIMIT 1),
        (SELECT t_id FROM {schema}.gc_prediotipo WHERE ilicode = 'Predio.Privado' LIMIT 1)
    ) AS tipo_predio,

    -- condicion_predio: Coincidencia EXACTA
    COALESCE(
        (SELECT t_id FROM {schema}.gc_condicionprediotipo WHERE ilicode = p.condicion_predio LIMIT 1),
        (SELECT t_id FROM {schema}.gc_condicionprediotipo WHERE ilicode = 'NPH' LIMIT 1)
    ) AS condicion_predio,

    -- destinacion_economica: Coincidencia EXACTA (diferencia Pecuario vs Agropecuario)
    -- Si no coincide, usa lógica por numero_predial (pos 6-7): '00' = Rural -> Agricola, otro = Urbano -> Habitacional
    COALESCE(
        (SELECT t_id FROM {schema}.gc_destinacioneconomicatipo WHERE ilicode = p.destinacion_economica LIMIT 1),
        CASE
            WHEN SUBSTRING(p.numero_predial_nacional FROM 6 FOR 2) = '00'
            THEN (SELECT t_id FROM {schema}.gc_destinacioneconomicatipo WHERE ilicode = 'Agricola' LIMIT 1)
            ELSE (SELECT t_id FROM {schema}.gc_destinacioneconomicatipo WHERE ilicode = 'Habitacional' LIMIT 1)
        END
    ) AS destinacion_economica,

    COALESCE(p.area_catastral_terreno::numeric, 0) AS area_catastral_terreno,
    p.area_registral::numeric AS area_registral_m2,

    -- vigencia_actualizacion_catastral: NOT NULL, usar fecha actual si es NULL
    COALESCE(p.vigencia_actualizacion_catastral::date, CURRENT_DATE) AS vigencia_actualizacion_catastral,

    -- estado: Valor fijo 'Activo'
    (SELECT t_id FROM {schema}.gc_estadotipo WHERE ilicode = 'Activo' LIMIT 1) AS estado,

    -- categoria_suelo: Coincidencia EXACTA (puede ser NULL)
    (SELECT t_id FROM {schema}.gc_categoriasuelotipo WHERE ilicode = p.categoria_suelo LIMIT 1) AS categoria_suelo,

    -- clase_suelo: Coincidencia EXACTA
    -- Si no coincide, usa lógica por numero_predial (pos 6-7): '00' = Rural, otro = Urbano
    COALESCE(
        (SELECT t_id FROM {schema}.gc_clasesuelotipo WHERE ilicode = p.clase_suelo LIMIT 1),
        CASE
            WHEN SUBSTRING(p.numero_predial_nacional FROM 6 FOR 2) = '00'
            THEN (SELECT t_id FROM {schema}.gc_clasesuelotipo WHERE ilicode = 'Rural' LIMIT 1)
            ELSE (SELECT t_id FROM {schema}.gc_clasesuelotipo WHERE ilicode = 'Urbano' LIMIT 1)
        END
    ) AS clase_suelo,

    p.numero_predial_anterior,
    p.area_construida::numeric,
    p.nombre,

    -- tipo (col_unidadadministrativabasicatipo): Valor fijo 'Informacion_Estadistica'
    (SELECT t_id FROM {schema}.col_unidadadministrativabasicatipo WHERE ilicode = 'Informacion_Estadistica' LIMIT 1) AS tipo,

    COALESCE(p.comienzo_vida_util_version::timestamp, NOW()) AS comienzo_vida_util_version,
    p.fin_vida_util_version::timestamp,
    COALESCE(p.espacio_de_nombres, 'gc_predio') AS espacio_de_nombres,
    COALESCE(p.id::varchar, p.numero_predial_nacional) AS local_id

FROM tmp_predio p;
